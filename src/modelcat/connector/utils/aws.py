from retry import retry

from . import run_cli_command, CLICommandError
import logging as log

from modelcat.consts import PRODUCT_S3_BUCKET


def check_awscli() -> bool:
    cmd = ["aws", "--version"]
    outputs = []
    try:
        run_cli_command(cmd, line_parser=lambda line: outputs.append(line))
        log.info(f'awscli version: {" ".join(outputs)}')
    except CLICommandError:
        return False

    return True


def check_aws_configuration(verbose: int = 0) -> bool:
    if check_awscli():
        log.info("awscli installation found")
    else:
        log.info(
            "`awscli` does not seem installed on the system. Please run `modelcat_setup` to properly configure your machine."
        )
        return False

    return True


@retry(exceptions=Exception, delay=20, tries=6, backoff=1, logger=None)  # trying for 6 * 20 = 120 seconds
def check_s3_access(group_id: str, verbose: bool = False) -> None:

    cmd = [
        "aws",
        "s3",
        "ls",
        f"s3://{PRODUCT_S3_BUCKET}/account/{group_id}/",
    ]
    outputs = []
    try:
        run_cli_command(
            command=cmd,
            verbose=verbose,
            line_parser=lambda line: outputs.append(line.strip()),
        )
        print("S3 access verified")
    except CLICommandError:
        raise
