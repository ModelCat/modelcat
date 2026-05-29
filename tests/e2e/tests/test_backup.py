"""
Backup behavior tests.

Validates that auto fix operations create backups of modified files.
"""
import os.path as osp

import pytest


@pytest.mark.e2e
class TestBackupBehavior:

    def test_autofix_creates_backup_directory(self, cli, classification_ds):
        """K1: Auto-fix modifying files should create .backup/ with originals."""
        classification_ds.remove_infos_key("dataset_size")
        cli.validate(classification_ds.path, auto_fix=True)
        backup_dir = osp.join(classification_ds.path, ".backup")
        assert osp.isdir(backup_dir)
        assert osp.exists(osp.join(backup_dir, "dataset_infos.json"))

    def test_autofix_backup_message_in_stdout(self, cli, classification_ds):
        """K2: Stdout should mention backup when auto-fix modifies files."""
        classification_ds.remove_infos_key("dataset_size")
        result = cli.validate(classification_ds.path, auto_fix=True)
        assert result.stdout_contains("backup") or result.stdout_contains(".backup")

    def test_autofix2_creates_backup_for_coco(self, cli, classification_ds):
        """K3: Auto-fix-2 modifying COCO files should backup the annotation."""
        classification_ds.add_image_without_annotation("coco_test.json")
        cli.validate(classification_ds.path, auto_fix_2="y")
        backup_dir = osp.join(classification_ds.path, ".backup")
        assert osp.isdir(backup_dir), "Backup directory should be created after auto-fix-2"
        backup_ann = osp.join(backup_dir, "annotations", "coco_test.json")
        assert osp.exists(backup_ann), "Annotation file should be backed up before modification"
