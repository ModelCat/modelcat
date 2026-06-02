"""
Empty split validation tests.

A split that contains no images cannot be trained or evaluated on, so any
empty split (train/validation/test) must be reported as an error and must
not be signed.
"""
import pytest


@pytest.mark.e2e
class TestEmptySplits:

    def test_empty_train_split(self, cli, classification_ds):
        """An empty train split should produce an error and not pass."""
        classification_ds.empty_split("coco_train.json")
        result = cli.validate(classification_ds.path)
        assert not result.crashed, f"CLI crashed with traceback:\n{result.stderr}"
        assert result.has_error("empty")
        assert not result.passed

    def test_empty_test_split(self, cli, classification_ds):
        """An empty test split should produce an error and not pass."""
        classification_ds.empty_split("coco_test.json")
        result = cli.validate(classification_ds.path)
        assert not result.crashed, f"CLI crashed with traceback:\n{result.stderr}"
        assert result.has_error("empty")
        assert not result.passed

    def test_empty_validation_split(self, cli, classification_ds):
        """An empty validation split should produce an error and not pass."""
        classification_ds.empty_split("coco_validation.json")
        result = cli.validate(classification_ds.path)
        assert not result.crashed, f"CLI crashed with traceback:\n{result.stderr}"
        assert result.has_error("empty")
        assert not result.passed

    def test_empty_split_not_silenced_by_autofix(self, cli, classification_ds):
        """Auto-fix only repairs metadata; an empty split must still error."""
        classification_ds.empty_split("coco_test.json")
        result = cli.validate(classification_ds.path, auto_fix=True, auto_fix_2="y")
        assert result.has_error("empty")
        assert not result.passed

    def test_empty_split_produces_no_signature(self, cli, classification_ds):
        """An empty split must not yield a signature hash."""
        classification_ds.empty_split("coco_validation.json")
        result = cli.validate(classification_ds.path)
        assert result.has_error("empty")
        assert result.signature is None

    def test_empty_split_detection(self, cli, detection_ds):
        """Empty split errors for object detection datasets too."""
        detection_ds.empty_split("coco_train.json")
        result = cli.validate(detection_ds.path)
        assert not result.crashed, f"CLI crashed with traceback:\n{result.stderr}"
        assert result.has_error("empty")
        assert not result.passed

    def test_empty_split_keypoints(self, cli, keypoints_ds):
        """Empty split errors for keypoints datasets too."""
        keypoints_ds.empty_split("coco_test.json")
        result = cli.validate(keypoints_ds.path)
        assert not result.crashed, f"CLI crashed with traceback:\n{result.stderr}"
        assert result.has_error("empty")
        assert not result.passed
