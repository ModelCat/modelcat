"""
COCO annotation file validation tests.

Validates param checks, schema integrity, category/label matching,
and duplicate ID detection within annotation files.
"""
import os

import pytest

from tests.e2e.fixtures.dataset_mutators import load_json, save_json


@pytest.mark.e2e
class TestCocoAnnotationFiles:

    def test_annotation_file_missing_on_disk(self, cli, classification_ds):
        """E1: Annotation file referenced in splits but not on disk should error cleanly."""
        ann_files = classification_ds.get_all_ann_files()
        if ann_files:
            os.remove(ann_files[0])
        result = cli.validate(classification_ds.path)
        assert not result.crashed, f"CLI crashed with traceback instead of clean error:\n{result.stderr}"
        assert result.has_error("missing")
        assert not result.passed

    def test_annotation_file_invalid_json(self, cli, classification_ds):
        """E2: Annotation file with invalid JSON should produce a clean error."""
        ann_files = classification_ds.get_all_ann_files()
        if ann_files:
            ann_name = ann_files[0].split("/")[-1]
            classification_ds.corrupt_coco(ann_name)
        result = cli.validate(classification_ds.path)
        assert not result.crashed, f"CLI crashed with traceback instead of clean error:\n{result.stderr}"
        assert result.has_error("not formatted correctly")
        assert not result.passed

    def test_missing_categories_section(self, cli, classification_ds):
        """E3: Missing 'categories' section should produce a param error."""
        classification_ds.remove_coco_section("coco_test.json", "categories")
        result = cli.validate(classification_ds.path)
        assert result.has_error("categories")
        assert not result.passed

    def test_missing_images_section(self, cli, classification_ds):
        """E4: Missing 'images' section should produce a clean param error."""
        classification_ds.remove_coco_section("coco_test.json", "images")
        result = cli.validate(classification_ds.path)
        assert not result.crashed, f"CLI crashed with traceback instead of clean error:\n{result.stderr}"
        assert result.has_error("images")
        assert not result.passed

    def test_missing_annotations_section(self, cli, classification_ds):
        """E5: Missing 'annotations' section should produce a param error."""
        classification_ds.remove_coco_section("coco_test.json", "annotations")
        result = cli.validate(classification_ds.path)
        assert result.has_error("annotations")
        assert not result.passed

    def test_missing_category_id(self, cli, classification_ds):
        """E5b: Missing categories.id should produce a clean error."""
        classification_ds.remove_coco_category_field("coco_test.json", "id")
        result = cli.validate(classification_ds.path)
        assert not result.crashed, f"CLI crashed with traceback instead of clean error:\n{result.stderr}"
        assert result.has_error("categories.id")
        assert not result.passed

    def test_missing_image_id(self, cli, classification_ds):
        """E5c: Missing images.id should produce a clean error."""
        classification_ds.remove_coco_image_field("coco_test.json", "id")
        result = cli.validate(classification_ds.path)
        assert not result.crashed, f"CLI crashed with traceback instead of clean error:\n{result.stderr}"
        assert result.has_error("images.id")
        assert not result.passed

    def test_missing_image_width_without_autofix2(self, cli, classification_ds):
        """E6: Missing images.width without auto-fix-2 should error (from screenshot scenario)."""
        classification_ds.remove_coco_image_field("coco_test.json", "width")
        result = cli.validate(classification_ds.path)
        assert result.has_error("images.width")
        assert not result.passed

    def test_missing_image_width_with_autofix2_y(self, cli, classification_ds):
        """E7: Missing images.width with --auto-fix-2 y should auto-fill from disk."""
        classification_ds.remove_coco_image_field("coco_test.json", "width")
        result = cli.validate(classification_ds.path, auto_fix_2="y")
        assert not result.has_error("images.width")

    def test_missing_image_height_without_autofix2(self, cli, classification_ds):
        """E8: Missing images.height without auto-fix-2 should error."""
        classification_ds.remove_coco_image_field("coco_test.json", "height")
        result = cli.validate(classification_ds.path)
        assert result.has_error("images.height")
        assert not result.passed

    def test_missing_image_height_with_autofix2_y(self, cli, classification_ds):
        """E8b: Missing images.height with --auto-fix-2 y should auto-fill from disk."""
        classification_ds.remove_coco_image_field("coco_test.json", "height")
        result = cli.validate(classification_ds.path, auto_fix_2="y")
        assert not result.has_error("images.height")

    def test_missing_image_filename(self, cli, classification_ds):
        """E9: Missing images.file_name should produce a clean error."""
        classification_ds.remove_coco_image_field("coco_test.json", "file_name")
        result = cli.validate(classification_ds.path)
        assert not result.crashed, f"CLI crashed with traceback instead of clean error:\n{result.stderr}"
        assert result.has_error("file_name")
        assert not result.passed

    def test_missing_category_name(self, cli, classification_ds):
        """E10: Missing categories.name should produce an error."""
        classification_ds.remove_coco_category_field("coco_test.json", "name")
        result = cli.validate(classification_ds.path)
        assert result.has_error("categories.name") or result.has_error("name")
        assert not result.passed

    def test_missing_annotation_id(self, cli, classification_ds):
        """E11a: Missing annotations.id should produce a clean error."""
        classification_ds.remove_coco_annotation_field("coco_test.json", "id")
        result = cli.validate(classification_ds.path)
        assert not result.crashed, f"CLI crashed with traceback instead of clean error:\n{result.stderr}"
        assert result.has_error("annotations.id")
        assert not result.passed

    def test_missing_annotation_image_id(self, cli, classification_ds):
        """E11b: Missing annotations.image_id should produce a clean error."""
        classification_ds.remove_coco_annotation_field("coco_test.json", "image_id")
        result = cli.validate(classification_ds.path)
        assert not result.crashed, f"CLI crashed with traceback instead of clean error:\n{result.stderr}"
        assert result.has_error("image_id")
        assert not result.passed

    def test_missing_annotation_category_id(self, cli, classification_ds):
        """E11c: Missing annotations.category_id should produce a clean error."""
        classification_ds.remove_coco_annotation_field("coco_test.json", "category_id")
        result = cli.validate(classification_ds.path)
        assert not result.crashed, f"CLI crashed with traceback instead of clean error:\n{result.stderr}"
        assert result.has_error("category_id")
        assert not result.passed

    def test_category_names_mismatch_labels(self, cli, classification_ds):
        """E12: Category names not matching dataset_infos labels should error."""
        classification_ds.mismatch_category_labels("coco_test.json")
        result = cli.validate(classification_ds.path)
        assert result.has_error("category names") or result.has_error("label names")
        assert not result.passed

    def test_duplicate_category_ids(self, cli, classification_ds):
        """E13: Duplicate category IDs should produce an error."""
        classification_ds.add_duplicate_category_id("coco_test.json")
        result = cli.validate(classification_ds.path)
        assert result.has_error("duplicate") or result.has_error("category")
        assert not result.passed

    def test_duplicate_image_ids(self, cli, classification_ds):
        """E14: Duplicate image IDs in a COCO file should produce an error."""
        ann_path = classification_ds.ann_file_path("coco_test.json")
        data = load_json(ann_path)
        if data["images"]:
            dup = dict(data["images"][0])
            dup["file_name"] = "dup_" + dup["file_name"]
            data["images"].append(dup)
        save_json(ann_path, data)
        result = cli.validate(classification_ds.path)
        assert result.has_error("duplicate image") or result.has_error("duplicate")
        assert not result.passed

    def test_duplicate_annotation_ids(self, cli, classification_ds):
        """E15: Duplicate annotation IDs should produce an error."""
        classification_ds.add_duplicate_annotation_id("coco_test.json")
        result = cli.validate(classification_ds.path)
        assert result.has_error("duplicate annotation") or result.has_error("duplicate")
        assert not result.passed

    def test_annotation_references_nonexistent_image(self, cli, classification_ds):
        """E16a: Annotation referencing non-existent image_id should error."""
        classification_ds.add_orphan_annotation("coco_test.json")
        result = cli.validate(classification_ds.path)
        assert result.has_error("non-existent image") or result.has_error("image")
        assert not result.passed

    def test_annotation_references_nonexistent_category(self, cli, classification_ds):
        """E16b: Annotation referencing non-existent category_id should error."""
        classification_ds.add_orphan_category_annotation("coco_test.json")
        result = cli.validate(classification_ds.path)
        assert result.has_error("non-existent category") or result.has_error("category")
        assert not result.passed
