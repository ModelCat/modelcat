"""
Thumbnail validation tests.

Validates auto-generation of thumbnail.jpg when missing,
and error handling when no images are available at all.
"""
import os.path as osp

import pytest


@pytest.mark.e2e
class TestThumbnail:

    def test_missing_thumbnail_auto_generated(self, cli, classification_ds):
        """H1: Missing thumbnail.jpg with images should auto-generate it."""
        classification_ds.ensure_no_thumbnail()
        result = cli.validate(classification_ds.path)
        assert osp.exists(osp.join(classification_ds.path, "thumbnail.jpg"))
        assert not result.has_error("thumbnail")

    def test_missing_thumbnail_no_images(self, cli, classification_ds):
        """H2: Missing thumbnail.jpg with no images should produce an error."""
        classification_ds.ensure_no_thumbnail()
        classification_ds.remove_all_images()
        result = cli.validate(classification_ds.path)
        assert result.has_error("thumbnail") or result.has_error("image")

    def test_existing_thumbnail_no_action(self, cli, classification_ds):
        """H3: Existing thumbnail.jpg should not trigger any thumbnail messages."""
        classification_ds.create_thumbnail()
        result = cli.validate(classification_ds.path)
        assert not result.has_error("thumbnail")
