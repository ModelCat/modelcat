from modelcat.consts import PRODUCT_NAME, PRODUCT_URL
from modelcat.connector.utils.consts import (
    PACKAGE_NAME,
    OAUTH_TOKEN_RE,
)
from modelcat.connector.utils.api import ProductAPIClient, APIConfig, APIError
from modelcat.connector.utils.aws import check_awscli, check_aws_configuration
from pathlib import Path
import os.path as osp
import os
import json
import re
import uuid
import argparse
import logging
from getpass_asterisk.getpass_asterisk import getpass_asterisk as getpass


def mask_modelcat_token(token: str, show_prefix: int = 3, show_suffix: int = 3, mask_len: int = 12) -> str:
    """
    Validate and mask a token of the form: <int>_<40 hex chars>.
    Example output: 1_123********678
    """
    token_re = re.compile(OAUTH_TOKEN_RE)
    m = token_re.match(token or "")
    if not m:
        # Don’t leak invalid input; show a generic masked example length
        return "?:***" + "*" * mask_len + "***"

    group_id, hexpart = m.groups()             # e.g. "1", "123456...40hex..."
    head = hexpart[:show_prefix]
    tail = hexpart[-show_suffix:] if show_suffix else ""
    return f"{group_id}_{head}{'*' * mask_len}{tail}"


def run_setup(verbose: int = 0):
    print(f"Welcome to {PACKAGE_NAME} one-time setup wizard.")
    print("We'll get you started in just a few simple steps!")
    print("-" * 50)
    if not check_awscli():
        print("Error: AWS CLI was not detected on your system.")
        print("Please install it and run the setup program again")
        print(
            "For install instructions go to: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
        )
        exit(1)
    else:
        print("AWS CLI installation verified.")
        print("-" * 50)

    # setting up the modelcat paths
    modelcat_path = osp.join(Path.home(), f".{PRODUCT_NAME.lower()}")
    os.makedirs(modelcat_path, exist_ok=True)
    modelcat_config_path = osp.join(modelcat_path, "config.json")

    # loading previous cached values
    if osp.exists(modelcat_config_path):
        try:
            with open(modelcat_config_path) as fp:
                product_config_old = json.load(fp)
                assert "group_id" in product_config_old
                assert "oauth_token" in product_config_old
                group_id_old = product_config_old["group_id"]
                oauth_token_old = product_config_old["oauth_token"]
        except Exception:
            group_id_old = None
            oauth_token_old = None
    else:
        group_id_old = None
        oauth_token_old = None

    print(
        f"Please enter your {PRODUCT_NAME} credentials (Group ID and OAuth Token)."
        f"\nYou can obtain them at {PRODUCT_URL}/datasets#upload.\n")
    while 1:
        previous_group_clause = f" [{group_id_old}]" if group_id_old is not None else ""
        group_id = input(f"{PRODUCT_NAME} Group ID{previous_group_clause}: ")
        if group_id == "" and group_id_old is not None:
            group_id = group_id_old
        try:
            uuid.UUID(str(group_id))
            break
        except Exception:
            print(
                f"Oops... This does not look right. `{PRODUCT_NAME} Account ID` should be a valid UUID in XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX format"
            )

    while 1:
        previous_token_clause = f" [{mask_modelcat_token(oauth_token_old)}]" if oauth_token_old is not None else ""

        try:
            oauth_token = getpass(f"{PRODUCT_NAME} OAuth Token{previous_token_clause}: ")
        except EOFError:
            # if user enters an empty string, use the old value
            oauth_token = ""

        if oauth_token == "" and oauth_token_old is not None:
            oauth_token = oauth_token_old
        if re.match(OAUTH_TOKEN_RE, oauth_token):
            break
        print(
            f"Oops... This does not look right. "
            f"`{PRODUCT_NAME} OAuth Token` should be an integer followed by an underscore, "
            f"followed by a 40 character string e.g.: 1_1234567890abcdef1234567890abcdef12345678"
        )

    api_config = APIConfig(
        base_url=PRODUCT_URL,
        oauth_token=oauth_token,
    )
    api_client = ProductAPIClient(api_config)

    # validate the user via OAuth token
    try:
        creds = api_client.get_me()
        print(f"\nSuccessfully authenticated as {creds['full_name']}")
    except APIError:
        print(
            f"Failed to validate the user via OAuth token. "
            f"Please obtain a valid token at {PRODUCT_URL}/datasets#upload."
        )
        exit(1)

    if not check_aws_configuration(verbose):
        print("Configuration failed.")

    # create the config file
    product_config = {
        "group_id": group_id,
        "oauth_token": oauth_token,
    }

    with open(modelcat_config_path, "w") as fp:
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

    parser = argparse.ArgumentParser(description="Run ModelCat setup wizard.")
    parser.add_argument("--debug", default=False, action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        verbose = 1
    else:
        logging.basicConfig(level=logging.WARNING)
        verbose = 0

    run_setup(verbose=verbose)


if __name__ == "__main__":
    setup_cli()
