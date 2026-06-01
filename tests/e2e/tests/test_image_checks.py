"""
Image-level check tests.

Validates missing images on disk, images without annotations,
duplicate images by content, and categories with no annotations.
"""
import pytest

from tests.e2e.fixtures.dataset_mutators import load_json, save_json


@pytest.mark.e2e
class TestImageChecks:

    def test_image_referenced_but_missing_on_disk(self, cli, classification_ds):
        """F1: Image referenced in annotation but not on disk should error."""
        removed = classification_ds.remove_image_file("coco_test.json", image_index=0)
        assert removed is not None
        result = cli.validate(classification_ds.path)
        assert result.has_error("not found") or result.has_error("missing")

    def test_image_without_annotation_no_autofix(self, cli, classification_ds):
        """F2: Image with no annotation (no auto-fix-2) should produce a warning."""
        classification_ds.add_image_without_annotation("coco_test.json")
        result = cli.validate(classification_ds.path)
        assert result.has_warning("don't have corresponding annotations") or result.has_warning("without")

    def test_image_without_annotation_autofix2_y(self, cli, classification_ds):
        """F3: Image with no annotation and --auto-fix-2 y should auto-remove it."""
        classification_ds.add_image_without_annotation("coco_test.json")
        result = cli.validate(classification_ds.path, auto_fix_2="y")
        assert not result.has_warning("without")

    def test_duplicate_image_filenames_in_split(self, cli, classification_ds):
        """F5: Duplicate image file_name entries within one split should warn or error."""
        classification_ds.add_duplicate_image_entry("coco_test.json")
        result = cli.validate(classification_ds.path)
        assert result.has_warning("duplicat") or result.has_error("duplicat") or result.has_error("duplicate")

    def test_category_no_annotations_in_train(self, cli, classification_ds):
        """F6: A category with zero annotations in train split should error."""
        ann_path = classification_ds.ann_file_path("coco_train.json")
        data = load_json(ann_path)
        if data["categories"]:
            target_cat_id = data["categories"][0]["id"]
            data["annotations"] = [
                a for a in data["annotations"] if a["category_id"] != target_cat_id
            ]
        save_json(ann_path, data)
        result = cli.validate(classification_ds.path)
        assert result.has_error("no images") or result.has_error("categories with no")
