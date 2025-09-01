import logging as log
import os
import os.path as osp
from modelcat.connector.utils import hash_dataset, run_cli_command
import re
import json
import math
import uuid
from tqdm import tqdm
import argparse
from pathlib import Path

from modelcat.consts import PRODUCT_NAME, PRODUCT_S3_BUCKET, PRODUCT_URL
from modelcat.connector.utils.api import APIConfig, ProductAPIClient, APIError
from modelcat.connector.utils.consts import PACKAGE_NAME, DEFAULT_AWS_PROFILE

from modelcat.connector.utils.aws import check_aws_configuration, check_s3_access
import pkg_resources


class DatasetUploader:
    def __init__(
        self,
        dataset_root_dir: str,
        group_id: str,
        oauth_token: str = None,
        ignore_validation: bool = False,
        verbose: int = 0,  # 1 for info, 2 for debug
    ):
        self.dataset_root = dataset_root_dir
        self.group_id = group_id
        self.verbose = verbose
        self.oauth_token = oauth_token

        if not self.is_valid_uuid(self.group_id):
            print(
                f'Provided `{PRODUCT_NAME} Group ID` ({self.group_id}) does not have a correct format. '
                'It should be a valid UUID e.g. "461b1b66-8787-11ed-aff3-07f20767316e"'
            )
            exit(1)

        self.ignore_validation = ignore_validation

        if not osp.exists(dataset_root_dir):
            print(f"Path does not exists: {dataset_root_dir}")
            exit(1)

        if not check_aws_configuration():
            exit(1)

        if not self.dataset_check():
            exit(1)

        with open(osp.join(self.dataset_root, "dataset_infos.json")) as fp:
            self.dataset_infos = json.load(fp)
        self.dataset_name = self.normalize_ds_name(list(self.dataset_infos.keys())[0])
        self.s3_uri = f"s3://{PRODUCT_S3_BUCKET}/account/{self.group_id}/datasets/{self.dataset_name}/"

        try:
            check_s3_access(self.group_id, verbose=self.verbose > 0)
        except Exception:
            exit(1)

    def dataset_check(self):
        ds_infos = osp.join(self.dataset_root, "dataset_infos.json")
        validator_log_path = osp.join(self.dataset_root, "dataset_validator_log.txt")

        if not osp.exists(ds_infos):
            print(f"Dataset boiler plate not found: {ds_infos}")
            return False

        if self.ignore_validation:
            log.warning(
                f"Signature validation skipped (overriden by user). You dataset may not work on {PRODUCT_NAME}"
            )
            return True

        print("Veryfying dataset signature...")
        try:
            with open(validator_log_path) as fp:
                text = fp.read()
            signature_sha = self.get_sha(text)
        except FileNotFoundError:
            signature_sha = None

        if signature_sha is None:
            print(
                "Dataset validation mark not found. Please run validation script first."
            )
            return False

        sha = hash_dataset(self.dataset_root)

        log.info(f"Signature: {signature_sha}")
        log.info(f"Computed:  {sha}")

        if sha != signature_sha:
            print(f"Validation marks mismatch. Expected {sha}, found {signature_sha}")
            return False

        print("Done!")

        return True

    def upload_s3(self):
        log.info(f"Uploading to: {self.s3_uri}")

        num_files, size = self._count_files(
            self.dataset_root
        )  # len([name for name in os.listdir('.') if os.path.isfile(name)])
        print(f"Found {num_files} files in the dataset: {self._convert_size(size)}")

        cmd = [
            "aws",
            "s3",
            "sync",
            self.dataset_root,
            self.s3_uri,
            "--profile",
            DEFAULT_AWS_PROFILE,
        ]

        print("")
        with tqdm(total=num_files, position=0) as pbar_top, tqdm(
            total=1, bar_format="{desc}", position=1
        ) as pbar_bottom:

            def report_progress(line: str):
                # print(line)
                if line.startswith("upload: "):
                    pbar_top.update(1)
                    try:
                        file = "Uploading file: " + line[8:].split(" to ")[0]
                        # pbar_bottom.update(0)
                        pbar_bottom.set_description(file)
                    except Exception:
                        log.warning(f"Failed parsing line: {line}")

            run_cli_command(
                command=cmd,
                cwd=".",
                verbose=bool(self.verbose),
                line_parser=report_progress,
            )

        try:
            print(f"Registering dataset in {PRODUCT_NAME} platform...")
            api_config = APIConfig(
                base_url=PRODUCT_URL,
                oauth_token=self.oauth_token,
            )
            api_client = ProductAPIClient(api_config)
            register_data = api_client.register_dataset(
                name=self.dataset_name,
                s3_uri=self.s3_uri,
                dataset_infos=self.dataset_infos,
            )

            print("Running Dataset Analysis immediately after registration...")
            api_client.submit_dataset_analysis(
                dataset_uri=self.s3_uri,
                group_id=self.group_id,
                dataset_name=self.dataset_name,
            )
            print("-" * 100)
            print(
                f"Dataset uploaded with uuid '{register_data['uuid']}'. You can view your dataset at: "
                f"{PRODUCT_URL}/datasets/{self.group_id}/{self.dataset_name}"
            )
        except APIError as ae:
            print(f"{PRODUCT_NAME} API error: {ae}")
            exit(1)

    @staticmethod
    def _count_files(folder: str):
        total = 0
        size = 0
        for root, _, files in os.walk(folder):
            total += len(files)
            size += sum((osp.getsize(osp.join(root, f)) for f in files))
        return total, size

    @staticmethod
    def _convert_size(size_bytes):
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return "%s %s" % (s, size_name[i])

    @staticmethod
    def is_valid_uuid(value):
        try:
            uuid.UUID(str(value))
            return True
        except ValueError:
            return False

    @staticmethod
    def normalize_ds_name(name: str):
        return re.sub("[^a-zA-Z0-9_\.-]", "", name) # noqa W605

    @staticmethod
    def get_sha(text):
        """
        Example:
        Validation passed and signed: bc81a84a510d7452bc1798af3a0b4dc93a50f94c79d807fe2f26e53adb3b5790
        """
        try:
            sha = re.findall("Validation passed and signed: ([0-9a-z]{64})", text)[0]
            return sha
        except Exception:
            return None


def upload_cli():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--dataset_path",
        help="Path to the root directory of the dataset.",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--verbose", "-v", action="count", default=0, help="Verbosity level: -v, -vv"
    )

    args = parser.parse_args()

    print(
        f'{PACKAGE_NAME} (v{pkg_resources.get_distribution("modelcat").version}) '
        f'- dataset validation utility'.center(100)
    )
    print("\n" + "-" * 100)

    if args.verbose:
        print(f"Uploading dataset with args: {args}.")

    if args.verbose == 1:
        log.getLogger().setLevel(log.INFO)
        print(f"{' Logging level: INFO ':=^30}")
    elif args.verbose >= 2:
        log.getLogger().setLevel(log.DEBUG)
        print(f"{' Logging level: DEBUG ':=^30}")

    platform_path = osp.join(Path.home(), f".{PRODUCT_NAME.lower()}")
    with open(osp.join(platform_path, "config.json")) as fp:
        platform_config = json.load(fp)
    group_id = platform_config.get("group_id", None)
    oauth_token = platform_config.get("oauth_token", None)
    log.info(f"Group ID: {group_id}")

    dsu = DatasetUploader(
        group_id=group_id,
        oauth_token=oauth_token,
        dataset_root_dir=args.dataset_path,
        verbose=args.verbose,
    )

    dsu.upload_s3()


if __name__ == "__main__":
    upload_cli()
