"""
Happy path tests for all task types.

Validates that each sample dataset passes validation cleanly.
"""
import pytest


@pytest.mark.e2e
class TestHappyPaths:

    def test_classification_dataset_valid(self, cli, classification_ds):
        """L1: Classification sample dataset should pass validation."""
        result = cli.validate(classification_ds.path)
        assert result.passed
        assert result.error_count == 0
        assert result.signature is not None

    def test_object_detection_dataset_valid(self, cli, detection_ds):
        """L2: Object detection sample dataset should pass validation."""
        result = cli.validate(detection_ds.path)
        assert result.passed
        assert result.error_count == 0
        assert result.signature is not None

    def test_keypoints_dataset_valid(self, cli, keypoints_ds):
        """L3: Keypoints sample dataset should pass validation."""
        result = cli.validate(keypoints_ds.path)
        assert result.passed
        assert result.error_count == 0
        assert result.signature is not None

    def test_classification_with_all_autofix_flags(self, cli, classification_ds):
        """L4: Valid dataset with all auto-fix flags should still pass."""
        result = cli.validate(classification_ds.path, auto_fix=True, auto_fix_2="y")
        assert result.passed
        assert result.error_count == 0

    def test_classification_verbose_output(self, cli, classification_ds):
        """L5: Valid dataset with verbose should show args and pass."""
        result = cli.validate(classification_ds.path, verbose=1)
        assert result.passed
        assert "validating dataset with args" in result.stdout.lower()
