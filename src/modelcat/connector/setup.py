from modelcat.consts import PRODUCT_NAME, PRODUCT_URL
from modelcat.connector.utils import run_cli_command
from modelcat.connector.utils.consts import (
    DEFAULT_AWS_FORMAT,
    DEFAULT_AWS_REGION,
    DEFAULT_AWS_PROFILE,
    PACKAGE_NAME,
)
from modelcat.connector.utils.api import ProductAPIClient, APIConfig, APIError
from modelcat.connector.utils.aws import check_awscli, check_aws_configuration
from pathlib import Path
import os.path as osp
import os
import json
import re
import uuid
from getpass_asterisk.getpass_asterisk import getpass_asterisk as getpass


def run_setup(verbose: int = 0):
    print(f"Welcome to {PACKAGE_NAME} one-time setup wizard.")
    print("We'll get you started in just a few simple steps!")
    print("-" * 50)
    if not check_awscli():
        print("Error: AWS CLI was not detected on your system.")
        print("Please install it and run the setup program again")
        print(
            "For install instuctions go to: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
        )
        exit(1)
    else:
        print("AWS CLI installation verified.")
        print("-" * 50)

    while 1:
        group_id = input(f"{PRODUCT_NAME} Group ID: ")
        try:
            uuid.UUID(str(group_id))
            break
        except Exception:
            print(
                f"Oops... This does not look right. `{PRODUCT_NAME} Account ID` should be a valid UUID in XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX format"
            )

    while 1:
        oauth_token = getpass(f"{PRODUCT_NAME} OAuth Token: ")
        if re.match(r"^\d+_[a-f0-9]{40}$", oauth_token):
            break
        print(
            f"Oops... This does not look right. `{PRODUCT_NAME} OAuth Token` should be an integer followed by an underscore, followed by a 40 character string e.g.: 1_1234567890abcdef1234567890abcdef12345678"
        )

    # get the AWS access key credentials
    try:
        api_config = APIConfig(
            base_url=PRODUCT_URL,
            oauth_token=oauth_token,
        )
        api_client = ProductAPIClient(api_config)
        creds = api_client.get_aws_access(group_id)

        aws_access_key = creds["access_key_id"]
        aws_secret_access_key = creds["secret_access_key"]
    except APIError as ae:
        print(f"{PRODUCT_NAME} API error: {ae}")
        exit(1)

    # configure AWS CLI
    outputs = []

    def append_fn(line):
        outputs.append(line)

    try:
        cmd = [
            "aws",
            "configure",
            "set",
            "region",
            DEFAULT_AWS_REGION,
            "--profile",
            DEFAULT_AWS_PROFILE,
        ]
        run_cli_command(cmd, line_parser=append_fn)
        cmd = [
            "aws",
            "configure",
            "set",
            "format",
            DEFAULT_AWS_FORMAT,
            "--profile",
            DEFAULT_AWS_PROFILE,
        ]
        run_cli_command(cmd, line_parser=append_fn)
        cmd = [
            "aws",
            "configure",
            "set",
            "aws_access_key_id",
            aws_access_key,
            "--profile",
            DEFAULT_AWS_PROFILE,
        ]
        run_cli_command(cmd, line_parser=append_fn)
        cmd = [
            "aws",
            "configure",
            "set",
            "aws_secret_access_key",
            aws_secret_access_key,
            "--profile",
            DEFAULT_AWS_PROFILE,
        ]
        run_cli_command(cmd, line_parser=append_fn)
    except Exception as e:
        print(f"AWS configuration failure: {e}")
        if verbose:
            print("\n".join(outputs))
        exit(1)

    if not check_aws_configuration(verbose):
        print("Configuration failed.")

    print("-" * 50)
    # checking access to S3
    print("Verifying AWS access...")
    # some retries to let the AWS access key propagate
    from modelcat.connector.utils.aws import check_s3_access
    try:
        check_s3_access(group_id, verbose=verbose > 0)
    except Exception:
        print("Verification failed... Please check your credentials or contact customer support.")
        exit(1)
    print("Verification successful.")

    # create the config file
    product_config = {
        "group_id": group_id,
        "oauth_token": oauth_token,
    }

    modelcat_path = osp.join(Path.home(), f".{PRODUCT_NAME.lower()}")
    os.makedirs(modelcat_path, exist_ok=True)
    with open(osp.join(modelcat_path, "config.json"), "w") as fp:
        json.dump(product_config, fp, indent=4)

    print("-" * 50)
    print("Configuration complete.")
    print("")
    print("Now you can use:")
    print(
        f"\t`modelcat_validate` to check your dataset for errors and verify {PRODUCT_NAME} interoperability"
    )
    print(f"\t`modelcat_upload` to upload dataset to {PRODUCT_NAME} platform")


def setup_cli():
    run_setup(verbose=1)


if __name__ == "__main__":
    setup_cli()
