"""
Dataset structure validation tests.

Validates that modelcat_validate correctly reports errors
when required files or directories are missing.
"""
import pytest


@pytest.mark.e2e
class TestDatasetStructure:

    def test_nonexistent_path(self, cli, tmp_path):
        """B1: Non-existent dataset path should exit with error."""
        fake_path = str(tmp_path / "does_not_exist")
        result = cli.validate(fake_path)
        assert result.exit_code != 0
        assert "does not exist" in result.stdout.lower() or "does not exist" in result.stderr.lower()

    def test_missing_dataset_infos(self, cli, classification_ds):
        """B2: Missing dataset_infos.json should produce a critical error and stop."""
        classification_ds.remove_file("dataset_infos.json")
        result = cli.validate(classification_ds.path)
        assert result.has_error("dataset_infos.json")
        assert not result.passed

    def test_missing_annotations_dir(self, cli, classification_ds):
        """B3: Missing annotations/ directory should produce an error."""
        classification_ds.remove_dir("annotations")
        result = cli.validate(classification_ds.path)
        assert result.has_error("annotations")
        assert not result.passed

    def test_missing_images_dir(self, cli, classification_ds):
        """B4: Missing images/ directory should produce an error."""
        classification_ds.remove_dir("images")
        result = cli.validate(classification_ds.path)
        assert result.has_error("images")
        assert not result.passed

    def test_missing_both_annotations_and_images(self, cli, classification_ds):
        """B5: Missing both directories should report errors for both."""
        classification_ds.remove_dir("annotations")
        classification_ds.remove_dir("images")
        result = cli.validate(classification_ds.path)
        assert result.has_error("annotations")
        assert result.has_error("images")
        assert not result.passed
