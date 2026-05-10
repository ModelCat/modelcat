"""
Cross-split integrity tests.

Validates detection of data leakage between train/test/validation splits.
"""
import pytest


@pytest.mark.e2e
class TestCrossSplitIntegrity:

    def test_split_leakage_train_test(self, cli, classification_ds):
        """G1: Significant leakage between train and test should produce error or note."""
        classification_ds.inject_split_leakage("coco_train.json", "coco_test.json", count=5)
        result = cli.validate(classification_ds.path)
        assert result.has_error("leakage") or result.has_message("leakage")

    def test_split_leakage_test_validation(self, cli, classification_ds):
        """G2: Leakage between test and validation should produce error or note."""
        classification_ds.inject_split_leakage(
            "coco_test.json", "coco_validation.json", count=5
        )
        result = cli.validate(classification_ds.path)
        assert result.has_error("leakage") or result.has_message("leakage")

    def test_minor_split_leakage_detected(self, cli, classification_ds):
        """G3: Even 1 leaked image should be detected (may be error or note depending on split size)."""
        classification_ds.inject_split_leakage("coco_train.json", "coco_test.json", count=1)
        result = cli.validate(classification_ds.path)
        assert result.has_message("leakage"), "Leakage should be detected even for a single image"
