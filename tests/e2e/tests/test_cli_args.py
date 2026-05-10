"""
CLI argument parsing tests.

Validates that modelcat_validate handles argument combinations correctly,
including missing required args, valid invocations, and invalid flag usage.
"""
import pytest


@pytest.mark.e2e
class TestCLIArguments:

    def test_no_arguments(self, cli):
        """A1: Running with no arguments should exit with code 2 and show usage."""
        result = cli.run_raw([])
        assert result.exit_code == 2
        assert "usage" in result.stderr.lower()

    def test_missing_dataset_path(self, cli):
        """A2: Omitting -d should exit with code 2 and mention required argument."""
        result = cli.run_raw(["--auto-fix"])
        assert result.exit_code == 2
        assert "required" in result.stderr.lower() or "-d" in result.stderr

    def test_valid_dataset_path(self, cli, classification_ds):
        """A3: Valid dataset path should produce validation output (exit 0)."""
        result = cli.validate(classification_ds.path)
        assert result.exit_code == 0
        assert "ModelCatConnector" in result.stdout or "validation" in result.stdout.lower()

    def test_auto_fix_flag_accepted(self, cli, classification_ds):
        """A4: --auto-fix flag is accepted without error."""
        result = cli.validate(classification_ds.path, auto_fix=True)
        assert result.exit_code == 0

    def test_auto_fix_2_y_accepted(self, cli, classification_ds):
        """A5: --auto-fix-2 y is accepted without error."""
        result = cli.validate(classification_ds.path, auto_fix_2="y")
        assert result.exit_code == 0

    def test_auto_fix_2_without_value(self, cli, classification_ds):
        """A6: --auto-fix-2 without a value should exit with argparse error."""
        result = cli.run_raw(["-d", classification_ds.path, "--auto-fix-2"])
        assert result.exit_code == 2
        assert "expected one argument" in result.stderr.lower() or "error" in result.stderr.lower()

    def test_verbose_flag(self, cli, classification_ds):
        """A7: --verbose flag should produce additional output."""
        result = cli.validate(classification_ds.path, verbose=1)
        assert result.exit_code == 0
        assert "validating dataset with args" in result.stdout.lower()
