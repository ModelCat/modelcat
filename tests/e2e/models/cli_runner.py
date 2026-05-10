import subprocess
import sys
from typing import List, Optional

from tests.e2e.models.cli_result import CLIResult


class CLIRunner:
    """
    Wraps subprocess invocation of modelcat_validate.

    Usage:
        runner = CLIRunner()
        result = runner.validate("/path/to/dataset")
        result = runner.validate("/path/to/dataset", auto_fix=True, auto_fix_2="y", verbose=2)
    """

    COMMAND = "modelcat_validate"

    def validate(
        self,
        dataset_path: str,
        auto_fix: bool = False,
        auto_fix_2: Optional[str] = None,
        verbose: int = 0,
        extra_args: Optional[List[str]] = None,
    ) -> CLIResult:
        """
        Run modelcat_validate with the given options and return a parsed CLIResult.
        """
        cmd = [sys.executable, "-m", "modelcat.connector.validate"]
        cmd.extend(["-d", dataset_path])

        if auto_fix:
            cmd.append("--auto-fix")
        if auto_fix_2 is not None:
            cmd.extend(["--auto-fix-2", auto_fix_2])
        for _ in range(verbose):
            cmd.append("-v")
        if extra_args:
            cmd.extend(extra_args)

        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )

        return CLIResult(
            exit_code=proc.returncode,
            stdout=proc.stdout,
            stderr=proc.stderr,
        )

    def run_raw(self, args: List[str]) -> CLIResult:
        """
        Run modelcat_validate with raw argument list (for testing bad arg combos).
        """
        cmd = [sys.executable, "-m", "modelcat.connector.validate"] + args

        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )

        return CLIResult(
            exit_code=proc.returncode,
            stdout=proc.stdout,
            stderr=proc.stderr,
        )
