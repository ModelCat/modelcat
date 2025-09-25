import requests
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Union, Optional, Dict

from modelcat.connector.utils.consts import PACKAGE_NAME


@dataclass
class APIConfig:
    """Configuration for the API client"""
    base_url: Union[str, list[str]]
    oauth_token: Optional[str] = None
    timeout: int = 120
    max_retries: int = 3
    backoff_factor: float = 0.5
    retry_status_codes: tuple = (500, 502, 503, 504)


class APIError(Exception):
    """Base exception for API-related errors"""
    pass


class BaseAPIClient:
    """Base class for API clients with common functionality"""

    def __init__(self, config: APIConfig):
        self.config = config
        self._session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create and configure a requests session with retry logic"""
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=list(self.config.retry_status_codes)
        )

        session = requests.Session()
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _get_headers(self, additional_headers: Optional[Dict] = None) -> Dict:
        """Generate headers for the request"""
        headers = {
            'Content-Type': 'application/json'
        }

        # Add OAuth token if available
        if self.config.oauth_token:
            headers['Authorization'] = f"Bearer {self.config.oauth_token}"

        if additional_headers:
            headers.update(additional_headers)

        return headers

    def _make_request(
            self,
            method: str,
            endpoint: str,
            data: Optional[Dict] = None,
            params: Optional[Dict] = None,
            headers: Optional[Dict] = None,
            files: Optional[Dict] = None,
    ) -> Dict:
        """
        Make an HTTP request with error handling

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path
            data: Request body data
            params: Query parameters
            headers: Additional headers

        Returns:
            Dict: Response data

        Raises:
            APIError: If the request fails or returns an error
        """
        result = None
        try:
            headers = self._get_headers(headers)
            if files is not None:
                headers.pop('Content-Type', None)
                send_kwargs = {'data': data}
            else:
                send_kwargs = {'json': data}

            url = f"{self.config.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
            response = self._session.request(
                method=method,
                url=url,
                params=params,
                headers=headers,
                files=files,
                timeout=self.config.timeout,
                **send_kwargs,
            )
            try:
                result = response.json()
            except Exception:
                pass

            response.raise_for_status()

            # Handle API-specific success/error format
            if isinstance(result, dict) and not result.get('success', True):
                error_msg = '; '.join(result.get('errors', ['Unknown error']))
                raise APIError(f"API returned error: {error_msg}")

            return result

        except requests.exceptions.JSONDecodeError as e:
            raise APIError(f"Failed to parse API response: {str(e)}")
        except requests.exceptions.RequestException as e:
            if result is not None:
                raise APIError(f"""API request failed with the following details:
{json.dumps(result, indent=2)}"""
                               )
            raise APIError(f"Request failed: {str(e)}")

    def __del__(self):
        """Cleanup method to ensure session is closed"""
        if hasattr(self, '_session'):
            self._session.close()


class ProductAPIClient(BaseAPIClient):
    """Client for storage token related API endpoints"""

    def get_me(self):
        """
        Get information about the current user for API validation

        Returns:
            Dict containing user information:
                - user_id
                - full_name
                - email
                - creation_date
                - modification_date
                - last_seen_date
                - origin
                - groups
        """
        result = self._make_request(
            method="GET",
            endpoint="/api/users/me",
        )
        if not result:
            raise APIError("No data returned from API")

        required_fields = {
            "user_id",
            "full_name",
            "email",
            "origin",
            "groups",
        }
        if not all(field in result for field in required_fields):
            raise APIError("Missing required fields in API response")

        return result

    def get_aws_access(self, group_id: str) -> Dict:
        """
        Get storage token for a specific group

        Args:
            group_id: The group ID to generate token for

        Returns:
            Dict containing token data:
                - group_id
                - access_key_id
                - secret_access_key
                - expiration_date
        """
        result = self._make_request(
            method='POST',
            endpoint='/api/storage/token/generate',
            data={'groupId': group_id}
        )

        if not result or not result.get('data'):
            raise APIError("No data returned from API")

        required_fields = {'group_id', 'access_key_id', 'secret_access_key', 'expiration_date'}
        if not all(field in result['data'] for field in required_fields):
            raise APIError("Missing required fields in API response")

        return result['data']

    def register_dataset(self, name: str, s3_uri: str, dataset_infos: dict) -> dict:
        """
        Register a new dataset using the /api/datasets/register endpoint.

        Args:
            name: Name of the dataset.
            s3_uri: S3 URI string where the dataset lives.
            dataset_infos: Dict containing dataset metadata/info.

        Returns:
            Dict with the API response.

        Raises:
            APIError: If registration fails.
        """
        data = {
            "name": name,
            "path": s3_uri,
            "datasetInfos": dataset_infos,
        }
        result = self._make_request(
            method='POST',
            endpoint='/api/datasets/register',
            data=data
        )
        if not result.get('success', True):
            error_msg = '; '.join(result.get('errors', ['Dataset registration failed.']))
            raise APIError(f"Dataset registration error: {error_msg}")

        return result

    def update_dataset(
            self,
            dataset_uuid: str,
            dataset_infos: dict,
            hidden: bool = False,
            task_types: Optional[list[str]] = None,
            access: Optional[dict] = None,
    ):
        body = {"datasetInfos": dataset_infos}
        if hidden is not None:
            body["hidden"] = hidden
        if task_types is not None:
            body["taskTypes"] = task_types
        if access is not None:
            body["access"] = access

        result = self._make_request(
            method="PUT",
            endpoint=f"/api/datasets/{dataset_uuid}",
            data=body,
        )

        if not result.get("success", True):
            error_msg = "; ".join(result.get("errors", ["Dataset update failed."]))
            raise APIError(f"Dataset update error: {error_msg}")

        return result

    def list_datasets(
        self,
        fields: Optional[list[str]] = None,
        include_dataset_infos: bool = False
    ):
        """
        List all user-visible datasets using the /api/datasets/list endpoint.

        Args:
            fields: List of metadata fields to include in the response (e.g. ["description", "splits"]).
            include_dataset_infos: Whether to include dataset metadata/info in the response.

        Returns:
            List of datasets visible to the current user.
        """
        params = {}
        if fields:
            params["fields"] = ",".join(fields)
        if include_dataset_infos:
            params["includeDatasetInfos"] = "true"

        result = self._make_request(
            method="GET",
            endpoint="/api/datasets/list",
            params=params
        )

        return result

    def submit_dataset_analysis(
            self,
            dataset_uri: str,
            group_id: str,
            dataset_name: Optional[str] = None,
            job_name: Optional[str] = None,
            env: Optional[str] = None,
    ):
        now_utc = datetime.now(timezone.utc)
        now_utc_str = now_utc.strftime('%b %d, %Y %H:%M UTC')
        data: dict = {
            "jobType": "Dataset Analysis",
            "jobDescription": f"Automatically generated by {PACKAGE_NAME} on {now_utc_str}",
            "board": "Virtual",
            "parameters": {
                "dataset": [
                    {
                        "path": dataset_uri,
                    }
                ]
            },
            "groups": [group_id],
            "notesStructured": "",
            "outputGroupId": group_id,
        }

        if job_name is not None:
            data["jobName"] = job_name
        elif dataset_name is not None:
            data["jobName"] = f"Dataset Analysis on '{dataset_name}'"

        if env is not None:
            data["jobName"] = f"[{env}] {data['jobName']}"

        data["parameters"] = json.dumps(data["parameters"])

        result = self._make_request(
            method='POST',
            endpoint='/api/submit',
            data=data,
            files={'file': (None, 'data')},
        )
        if not result.get('success', True):
            error_msg = '; '.join(result.get('errors', ['Job submission failed.']))
            raise APIError(f"Job submission error: {error_msg}")

        return result
