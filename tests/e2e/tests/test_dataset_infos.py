"""
dataset_infos.json content and metadata validation tests.

Covers missing/invalid keys, task templates, splits, and size/count metadata.
"""
import pytest

from tests.e2e.fixtures.dataset_mutators import load_json, save_json


@pytest.mark.e2e
class TestDatasetInfosContent:

    def test_invalid_json(self, cli, classification_ds):
        """C1: Corrupted dataset_infos.json should produce a clean error."""
        classification_ds.corrupt_dataset_infos()
        result = cli.validate(classification_ds.path)
        assert not result.crashed, f"CLI crashed with traceback instead of clean error:\n{result.stderr}"
        assert result.has_error("not formatted correctly")
        assert not result.passed

    def test_missing_splits_key(self, cli, classification_ds):
        """C2: Missing 'splits' key should produce an error."""
        classification_ds.remove_infos_key("splits")
        result = cli.validate(classification_ds.path)
        assert result.has_error("splits")
        assert not result.passed

    def test_missing_task_templates_key(self, cli, classification_ds):
        """C3: Missing 'task_templates' key should produce an error."""
        classification_ds.remove_infos_key("task_templates")
        result = cli.validate(classification_ds.path)
        assert result.has_error("task_templates")
        assert not result.passed

    def test_missing_description_without_autofix(self, cli, classification_ds):
        """C4: Missing 'description' without auto-fix should produce a warning."""
        classification_ds.remove_infos_key("description")
        result = cli.validate(classification_ds.path)
        assert result.has_warning("description")

    def test_missing_description_with_autofix(self, cli, classification_ds):
        """C5: Missing 'description' with --auto-fix should be silently fixed."""
        classification_ds.remove_infos_key("description")
        result = cli.validate(classification_ds.path, auto_fix=True)
        assert not result.has_warning("description")

    def test_missing_builder_name_without_autofix(self, cli, classification_ds):
        """C6: Missing 'builder_name' without auto-fix should produce a warning."""
        classification_ds.remove_infos_key("builder_name")
        result = cli.validate(classification_ds.path)
        assert result.has_warning("builder_name")

    def test_missing_config_name_without_autofix(self, cli, classification_ds):
        """C6b: Missing 'config_name' without auto-fix should produce a warning."""
        classification_ds.remove_infos_key("config_name")
        result = cli.validate(classification_ds.path)
        assert result.has_warning("config_name")

    def test_missing_config_name_with_autofix(self, cli, classification_ds):
        """C6c: Missing 'config_name' with --auto-fix should be silently fixed."""
        classification_ds.remove_infos_key("config_name")
        result = cli.validate(classification_ds.path, auto_fix=True)
        assert not result.has_warning("config_name")

    def test_missing_train_split(self, cli, classification_ds):
        """C7: Missing 'train' split should produce an error."""
        classification_ds.remove_infos_key("splits.train")
        result = cli.validate(classification_ds.path)
        assert result.has_error("train")
        assert not result.passed

    def test_missing_test_split(self, cli, classification_ds):
        """C8: Missing 'test' split should produce an error."""
        classification_ds.remove_infos_key("splits.test")
        result = cli.validate(classification_ds.path)
        assert result.has_error("test")
        assert not result.passed

    def test_missing_validation_split(self, cli, classification_ds):
        """C9: Missing 'validation' split should produce an error."""
        classification_ds.remove_infos_key("splits.validation")
        result = cli.validate(classification_ds.path)
        assert result.has_error("validation")
        assert not result.passed

    def test_split_missing_dataset_name(self, cli, classification_ds):
        """C10: Split missing 'dataset_name' key should produce an error."""
        classification_ds.remove_infos_key("splits.train.dataset_name")
        result = cli.validate(classification_ds.path)
        assert result.has_error("dataset_name")
        assert not result.passed

    def test_empty_task_templates(self, cli, classification_ds):
        """C11: Empty task_templates list should produce an error."""
        classification_ds.set_infos_value("task_templates", [])
        result = cli.validate(classification_ds.path)
        assert result.has_error("task templates")
        assert not result.passed

    def test_invalid_task_type(self, cli, classification_ds):
        """C12: Invalid task type should produce an error with allowed values."""
        classification_ds.set_infos_value("task_templates.0.task", "segmentation")
        result = cli.validate(classification_ds.path)
        assert result.has_error("classification")
        assert not result.passed

    def test_missing_labels_in_task_template(self, cli, classification_ds):
        """C13: Missing 'labels' key in task_templates should produce an error."""
        classification_ds.remove_infos_key("task_templates.0.labels")
        result = cli.validate(classification_ds.path)
        assert result.has_error("labels")
        assert not result.passed

    def test_multiple_task_templates(self, cli, classification_ds):
        """C14: Multiple task_templates should produce a pydantic validation error."""
        data = load_json(classification_ds.dataset_infos_path)
        ds_name = list(data.keys())[0]
        template = data[ds_name]["task_templates"][0]
        data[ds_name]["task_templates"].append(dict(template))
        save_json(classification_ds.dataset_infos_path, data)
        result = cli.validate(classification_ds.path)
        assert result.has_error("validation error") or result.has_error("task_templates")
        assert not result.passed


@pytest.mark.e2e
class TestSizeAndCountMetadata:

    def test_dataset_size_mismatch_warning(self, cli, classification_ds):
        """D1: Incorrect dataset_size without auto-fix should produce a warning."""
        classification_ds.set_infos_value("dataset_size", 999999)
        result = cli.validate(classification_ds.path)
        assert result.has_warning("dataset_size") or result.has_warning("images")

    def test_dataset_size_mismatch_autofix(self, cli, classification_ds):
        """D2: Incorrect dataset_size with --auto-fix should be silently corrected."""
        classification_ds.set_infos_value("dataset_size", 999999)
        result = cli.validate(classification_ds.path, auto_fix=True)
        assert not result.has_warning("dataset_size")

    def test_missing_dataset_size_warning(self, cli, classification_ds):
        """D3: Missing dataset_size without auto-fix should produce a warning."""
        classification_ds.remove_infos_key("dataset_size")
        result = cli.validate(classification_ds.path)
        assert result.has_warning("dataset_size")

    def test_size_in_bytes_mismatch_warning(self, cli, classification_ds):
        """D4: Incorrect size_in_bytes without auto-fix should produce a warning."""
        classification_ds.set_infos_value("size_in_bytes", 1)
        result = cli.validate(classification_ds.path)
        assert result.has_warning("size_in_bytes") or result.has_warning("dataset size")

    def test_missing_size_in_bytes_warning(self, cli, classification_ds):
        """D5: Missing size_in_bytes without auto-fix should produce a warning."""
        classification_ds.remove_infos_key("size_in_bytes")
        result = cli.validate(classification_ds.path)
        assert result.has_warning("size_in_bytes")

    def test_num_examples_mismatch_warning(self, cli, classification_ds):
        """D6: Incorrect num_examples for a split should produce a warning."""
        classification_ds.set_infos_value("splits.train.num_examples", 999999)
        result = cli.validate(classification_ds.path)
        assert result.has_warning("num_examples") or result.has_warning("images")

    def test_num_bytes_mismatch_warning(self, cli, classification_ds):
        """D7: Incorrect num_bytes for a split should produce a warning."""
        classification_ds.set_infos_value("splits.train.num_bytes", 1)
        result = cli.validate(classification_ds.path)
        assert result.has_warning("num_bytes") or result.has_warning("split")

    def test_missing_num_examples_warning(self, cli, classification_ds):
        """D8: Missing num_examples for a split should produce a warning."""
        classification_ds.remove_infos_key("splits.train.num_examples")
        result = cli.validate(classification_ds.path)
        assert result.has_warning("num_examples")

    def test_missing_num_bytes_warning(self, cli, classification_ds):
        """D9: Missing num_bytes for a split should produce a warning."""
        classification_ds.remove_infos_key("splits.train.num_bytes")
        result = cli.validate(classification_ds.path)
        assert result.has_warning("num_bytes")
