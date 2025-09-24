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
from datetime import datetime

from modelcat.consts import PRODUCT_NAME, PRODUCT_S3_BUCKET, PRODUCT_URL
from modelcat.connector.utils.api import APIConfig, ProductAPIClient, APIError
from modelcat.connector.utils.common import format_local_datetime
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
        self.s3_uri = f"s3://{PRODUCT_S3_BUCKET}/account/{self.group_id}/datasets/{str(uuid.uuid4())}/"

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

    def upload_s3(self, on_existing_dataset_name: str = None):
        api_config = APIConfig(
            base_url=PRODUCT_URL,
            oauth_token=self.oauth_token,
        )
        api_client = ProductAPIClient(api_config)

        print("Checking for an existing dataset with the same name...")

        datasets = api_client.list_datasets()

        datasets_same_name = [ds for ds in datasets if ds.get("name") == self.dataset_name]
        len_same_name = len(datasets_same_name)
        old_ds_uuid = None
        if len_same_name > 0:
            print(f"We found {len_same_name} dataset(s) with the same name.")
            if on_existing_dataset_name is None:
                for i, ds in enumerate(datasets_same_name):
                    creation_datetime = datetime.fromisoformat(
                        ds["creation_date"].replace("Z", "+00:00")
                    )
                    creation_datetime_nice = format_local_datetime(creation_datetime)
                    print(
                        f"[{i + 1}]  - {ds['name']} ({ds['uuid']}) created on {creation_datetime_nice} with URI {ds['path']}"
                    )
                print(
                    f"To proceed, enter one of the following options: "
                    f"\n  - 'n' to cancel the upload. You can change the dataset name in dataset_infos.json and try again."
                    f"\n  - 'y' to proceed with the upload. You will be able to view and use all datasets with the same name."
                    f"\n  - '{1}' - '{len_same_name}' to overwrite an existing dataset with that index."
                )
                choice = input("> ")
            else:
                choice = on_existing_dataset_name
            if choice == "n":
                print("Upload cancelled.")
                exit(1)
            elif choice == "y":
                print("Proceeding with the upload...")
            elif choice.isdigit() or choice == "o":
                # if the user wants to overwrite automatically but there are multiple datasets with the same name,
                # we can't select a random dataset.
                if choice == "o" and len_same_name == 1:
                    choice = "0"
                else:
                    print(f"Multiple datasets with the same name found. Aborting upload. "
                          f"Try rerunning in interactive mode to select which dataset to overwrite.")
                    exit(1)
                choice_idx = int(choice) - 1
                if choice_idx < 0 or choice_idx >= len_same_name:
                    print("Invalid choice.")
                    exit(1)
                self.s3_uri = datasets_same_name[choice_idx]["path"]
                print(f"Overwriting existing dataset with URI: {self.s3_uri}")
                old_ds_uuid = datasets_same_name[choice_idx]["uuid"]
        else:
            print("No datasets found with the same name. Uploading a new dataset...")

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
            if not old_ds_uuid:
                # we register new datasets
                print(f"Registering dataset in {PRODUCT_NAME} platform...")

                register_data = api_client.register_dataset(
                    name=self.dataset_name,
                    s3_uri=self.s3_uri,
                    dataset_infos=self.dataset_infos,
                )
                ds_uuid = register_data["uuid"]
            else:
                # we update old datasets
                print(f"Updating dataset in {PRODUCT_NAME} platform...")
                api_client.update_dataset(
                    dataset_uuid=old_ds_uuid,
                    dataset_infos=self.dataset_infos,
                )
                ds_uuid = old_ds_uuid

            print("Running Dataset Analysis immediately after registration...")
            api_client.submit_dataset_analysis(
                dataset_uri=self.s3_uri,
                group_id=self.group_id,
                dataset_name=self.dataset_name,
            )
            print("-" * 100)
            print(
                f"Dataset uploaded with uuid '{ds_uuid}'. You can view your dataset at: "
                f"{PRODUCT_URL}/datasets/{ds_uuid}"
            )
        except APIError as ae:
            print(f"Dataset registration/upload failed. {PRODUCT_NAME} API error: {ae}")
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
        "--on_existing_dataset_name",
        choices=["n", "y", "o"],
        default=None,
        help=(
            "Behavior when uploading a dataset with the same name. If omitted, user will be prompted.\n"
            "  n : Abort upload\n"
            "  y : Continue uploading with the same name\n"
            "  o : Overwrite existing dataset with the name "
            "(fails if multiple datasets have the same name)"
        ),
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

    dsu.upload_s3(on_existing_dataset_name=args.on_existing_dataset_name)


if __name__ == "__main__":
    upload_cli()
