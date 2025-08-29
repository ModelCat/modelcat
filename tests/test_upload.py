import unittest
from unittest.mock import patch, MagicMock, mock_open
import json

from modelcat.connector.upload import DatasetUploader, upload_cli
from modelcat.consts import PRODUCT_S3_BUCKET


class TestDatasetUploader(unittest.TestCase):
    """Test cases for the DatasetUploader class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a mock dataset_infos.json content
        self.mock_dataset_infos = {
            "test_dataset": {
                "description": "Test dataset",
                "citation": "",
                "homepage": "",
                "license": "",
                "features": {},
                "splits": {
                    "train": {
                        "name": "train",
                        "num_bytes": 1000,
                        "num_examples": 10
                    }
                },
                "supervised_keys": None,
                "builder_name": "test_builder",
                "dataset_size": 5000
            }
        }

        # Mock parameters
        self.dataset_root = "/fake/path"
        self.group_id = "12345678-1234-1234-1234-123456789012"
        self.oauth_token = "1_1234567890abcdef1234567890abcdef12345678"

    @patch('modelcat.connector.upload.check_aws_configuration')
    @patch('modelcat.connector.upload.osp.exists')
    @patch('builtins.open', new_callable=mock_open, read_data=json.dumps({"test_dataset": {
        "description": "Test dataset",
        "citation": "",
        "homepage": "",
        "license": "",
        "features": {},
        "splits": {
            "train": {
                "name": "train",
                "num_bytes": 1000,
                "num_examples": 10
            }
        },
        "supervised_keys": None,
        "builder_name": "test_builder",
        "dataset_size": 5000
    }}))
    @patch('modelcat.connector.upload.check_s3_access')
    @patch('modelcat.connector.upload.DatasetUploader.dataset_check')
    def test_init_success(
            self, mock_dataset_check, mock_check_s3_access,
            mock_open, mock_exists, mock_check_aws_config
    ):
        """Test successful initialization of DatasetUploader."""
        # Mock dependencies
        mock_check_aws_config.return_value = True
        mock_exists.return_value = True
        mock_dataset_check.return_value = True

        # Create uploader instance
        uploader = DatasetUploader(
            dataset_root_dir=self.dataset_root,
            group_id=self.group_id,
            oauth_token=self.oauth_token
        )

        # Verify initialization
        self.assertEqual(uploader.dataset_root, self.dataset_root)
        self.assertEqual(uploader.group_id, self.group_id)
        self.assertEqual(uploader.oauth_token, self.oauth_token)
        self.assertEqual(uploader.dataset_name, "test_dataset")
        self.assertEqual(uploader.s3_uri, f"s3://{PRODUCT_S3_BUCKET}/account/{self.group_id}/datasets/test_dataset/")

        # Verify dependencies were checked
        mock_check_aws_config.assert_called_once()
        mock_exists.assert_called_once_with(self.dataset_root)
        mock_dataset_check.assert_called_once()
        mock_check_s3_access.assert_called_once()

    @patch('modelcat.connector.upload.check_aws_configuration')
    @patch('modelcat.connector.upload.osp.exists')
    def test_init_invalid_uuid(self, mock_exists, mock_check_aws_config):
        """Test initialization with invalid UUID."""
        # Mock dependencies
        mock_check_aws_config.return_value = True
        mock_exists.return_value = True

        # Create uploader instance with invalid UUID
        with self.assertRaises(SystemExit) as cm:
            DatasetUploader(
                dataset_root_dir=self.dataset_root,
                group_id="invalid-uuid",
                oauth_token=self.oauth_token
            )

        # Verify exit code
        self.assertEqual(cm.exception.code, 1)

    @patch('modelcat.connector.upload.check_aws_configuration')
    @patch('modelcat.connector.upload.osp.exists')
    def test_init_path_not_exists(self, mock_exists, mock_check_aws_config):
        """Test initialization with non-existent path."""
        # Mock dependencies
        mock_check_aws_config.return_value = True
        mock_exists.return_value = False

        # Create uploader instance with non-existent path
        with self.assertRaises(SystemExit) as cm:
            DatasetUploader(
                dataset_root_dir=self.dataset_root,
                group_id=self.group_id,
                oauth_token=self.oauth_token
            )

        # Verify exit code
        self.assertEqual(cm.exception.code, 1)

    @patch('modelcat.connector.upload.osp.exists', return_value=True)
    @patch('modelcat.connector.upload.hash_dataset', return_value="test_sha_hash")
    @patch('modelcat.connector.upload.check_aws_configuration', return_value=True)
    @patch('modelcat.connector.upload.check_s3_access')
    def test_dataset_check_success(self, mock_check_s3_access, mock_check_aws_config, mock_hash_dataset, mock_exists):
        """Test successful dataset check."""
        # Skip the actual dataset_check in the constructor
        with patch.object(DatasetUploader, 'dataset_check', return_value=True):
            # Mock the file operations
            dataset_infos_mock = mock_open(read_data=json.dumps(self.mock_dataset_infos))
            validator_log_mock = mock_open(read_data="Validation passed and signed: test_sha_hash")

            # Create a side_effect function that returns different mock objects based on the file path
            def open_side_effect(file, *args, **kwargs):
                if 'dataset_infos.json' in str(file):
                    return dataset_infos_mock()
                elif 'dataset_validator_log.txt' in str(file):
                    return validator_log_mock()
                return mock_open().return_value

            # Apply the mock with side_effect
            with patch('builtins.open', side_effect=open_side_effect):
                # Create the uploader instance
                uploader = DatasetUploader(
                    dataset_root_dir=self.dataset_root,
                    group_id=self.group_id,
                    oauth_token=self.oauth_token
                )

                # Override the mocked dataset_check to test the actual method
                original_method = DatasetUploader.dataset_check
                uploader.dataset_check = lambda: original_method(uploader)

                # Run the dataset check
                result = uploader.dataset_check()

                # Verify result
                self.assertTrue(result)
                # hash_dataset is not called in dataset_check when validator_log exists
                # and contains a valid signature, so we don't assert it here

    @patch('modelcat.connector.upload.run_cli_command')
    @patch('modelcat.connector.upload.ProductAPIClient')
    @patch('modelcat.connector.upload.DatasetUploader.dataset_check', return_value=True)
    @patch('modelcat.connector.upload.check_aws_configuration', return_value=True)
    @patch('modelcat.connector.upload.osp.exists', return_value=True)
    @patch('modelcat.connector.upload.check_s3_access')
    @patch('modelcat.connector.upload.DatasetUploader._count_files', return_value=(10, 1000))
    def test_upload_s3_success(
            self, mock_count_files, mock_check_s3_access, mock_exists,
            mock_check_aws_config, mock_dataset_check, mock_api_client, mock_run_cli
    ):
        """Test successful S3 upload."""
        # Mock dependencies
        mock_client_instance = MagicMock()
        mock_client_instance.register_dataset.return_value = {"uuid": "test-uuid"}
        mock_api_client.return_value = mock_client_instance

        # Mock the file operations
        dataset_infos_mock = mock_open(read_data=json.dumps(self.mock_dataset_infos))

        # Apply the mock with side_effect
        with patch('builtins.open', return_value=dataset_infos_mock()):
            # Create uploader instance
            uploader = DatasetUploader(
                dataset_root_dir=self.dataset_root,
                group_id=self.group_id,
                oauth_token=self.oauth_token
            )

            # Run the upload
            uploader.upload_s3()

        # Verify AWS CLI command was run
        mock_run_cli.assert_called_once()

        # Verify API client was created and used
        mock_api_client.assert_called_once()
        mock_client_instance.register_dataset.assert_called_once_with(
            name="test_dataset",
            s3_uri=f"s3://{PRODUCT_S3_BUCKET}/account/{self.group_id}/datasets/test_dataset/",
            dataset_infos=self.mock_dataset_infos
        )
        mock_client_instance.submit_dataset_analysis.assert_called_once()

    @patch('modelcat.connector.upload.argparse.ArgumentParser.parse_args')
    @patch('modelcat.connector.upload.pkg_resources.get_distribution')
    @patch('modelcat.connector.upload.osp.join')
    @patch('builtins.open', new_callable=mock_open, read_data=json.dumps({
        "group_id": "12345678-1234-1234-1234-123456789012",
        "oauth_token": "1_1234567890abcdef1234567890abcdef12345678"
    }))
    @patch('modelcat.connector.upload.DatasetUploader')
    def test_upload_cli(self, mock_uploader, mock_file, mock_join, mock_get_dist, mock_parse_args):
        """Test the upload_cli function."""
        # Mock command line arguments
        args = MagicMock()
        args.dataset_path = self.dataset_root
        args.verbose = 1
        mock_parse_args.return_value = args

        # Mock package version
        mock_dist = MagicMock()
        mock_dist.version = "1.0.0"
        mock_get_dist.return_value = mock_dist

        # Mock path joining
        mock_join.return_value = "/fake/config/path"

        # Mock uploader instance
        mock_uploader_instance = MagicMock()
        mock_uploader.return_value = mock_uploader_instance

        # Run the CLI function
        upload_cli()

        # Verify uploader was created and used
        mock_uploader.assert_called_once_with(
            group_id=self.group_id,
            oauth_token=self.oauth_token,
            dataset_root_dir=self.dataset_root,
            verbose=1
        )
        mock_uploader_instance.upload_s3.assert_called_once()


if __name__ == '__main__':
    unittest.main()
