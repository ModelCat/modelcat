import os
import os.path as osp
import shutil
from typing import Any, List, Optional

from tests.e2e.fixtures import dataset_mutators as mut


class DatasetBuilder:
    """
    Copies a sample dataset into a temporary directory and provides
    chainable mutation methods to set up specific test scenarios.

    Usage:
        builder = DatasetBuilder(source, tmp_path)
        builder.remove_file("dataset_infos.json")
        builder.remove_coco_image_field("coco_test.json", "width")
        # then pass builder.path to CLIRunner
    """

    def __init__(self, source_dir: str, tmp_dir: str):
        self._source = source_dir
        self._path = osp.join(tmp_dir, osp.basename(source_dir))
        shutil.copytree(source_dir, self._path)
        self._remove_thumbnail_if_exists()

    def _remove_thumbnail_if_exists(self):
        """Remove pre-existing thumbnail so tests start clean (validator auto-generates it)."""
        thumb = osp.join(self._path, "thumbnail.jpg")
        if osp.exists(thumb):
            os.remove(thumb)

    @property
    def path(self) -> str:
        return self._path

    @property
    def dataset_infos_path(self) -> str:
        return osp.join(self._path, "dataset_infos.json")

    @property
    def annotations_dir(self) -> str:
        return osp.join(self._path, "annotations")

    @property
    def images_dir(self) -> str:
        return osp.join(self._path, "images")

    @property
    def dataset_name(self) -> str:
        data = mut.load_json(self.dataset_infos_path)
        return list(data.keys())[0]

    def ann_file_path(self, filename: str) -> str:
        return osp.join(self.annotations_dir, filename)

    def get_all_ann_files(self) -> List[str]:
        return mut.get_all_annotation_files(self._path)

    def get_first_ann_file(self) -> Optional[str]:
        return mut.get_first_annotation_file(self._path)

    # --- File-level mutations ---

    def remove_file(self, relative_path: str) -> "DatasetBuilder":
        mut.remove_file(self._path, relative_path)
        return self

    def remove_dir(self, relative_path: str) -> "DatasetBuilder":
        mut.remove_file(self._path, relative_path)
        return self

    # --- dataset_infos.json mutations ---

    def remove_infos_key(self, dotted_key: str) -> "DatasetBuilder":
        """Remove a key using dot notation relative to the dataset_infos root."""
        full_key = f"{self.dataset_name}.{dotted_key}"
        mut.remove_json_key(self.dataset_infos_path, full_key)
        return self

    def set_infos_value(self, dotted_key: str, value: Any) -> "DatasetBuilder":
        full_key = f"{self.dataset_name}.{dotted_key}"
        mut.set_json_value(self.dataset_infos_path, full_key, value)
        return self

    def corrupt_dataset_infos(self) -> "DatasetBuilder":
        mut.corrupt_json(self.dataset_infos_path)
        return self

    # --- COCO annotation mutations ---

    def remove_coco_image_field(
        self, ann_filename: str, field_name: str, image_index: int = 0
    ) -> "DatasetBuilder":
        mut.remove_coco_image_field(
            self.ann_file_path(ann_filename), field_name, image_index
        )
        return self

    def remove_coco_image_field_all(
        self, ann_filename: str, field_name: str
    ) -> "DatasetBuilder":
        mut.remove_coco_image_field_all(
            self.ann_file_path(ann_filename), field_name
        )
        return self

    def remove_coco_section(
        self, ann_filename: str, section: str
    ) -> "DatasetBuilder":
        mut.remove_coco_section(self.ann_file_path(ann_filename), section)
        return self

    def remove_coco_annotation_field(
        self, ann_filename: str, field_name: str, index: int = 0
    ) -> "DatasetBuilder":
        mut.remove_coco_annotation_field(
            self.ann_file_path(ann_filename), field_name, index
        )
        return self

    def remove_coco_category_field(
        self, ann_filename: str, field_name: str, index: int = 0
    ) -> "DatasetBuilder":
        mut.remove_coco_category_field(
            self.ann_file_path(ann_filename), field_name, index
        )
        return self

    def corrupt_coco(self, ann_filename: str) -> "DatasetBuilder":
        mut.corrupt_json(self.ann_file_path(ann_filename))
        return self

    def add_duplicate_image_entry(self, ann_filename: str) -> "DatasetBuilder":
        mut.add_duplicate_image_entry(self.ann_file_path(ann_filename))
        return self

    def add_duplicate_category_id(self, ann_filename: str) -> "DatasetBuilder":
        mut.add_duplicate_category_id(self.ann_file_path(ann_filename))
        return self

    def add_duplicate_annotation_id(self, ann_filename: str) -> "DatasetBuilder":
        mut.add_duplicate_annotation_id(self.ann_file_path(ann_filename))
        return self

    def add_orphan_annotation(self, ann_filename: str) -> "DatasetBuilder":
        mut.add_orphan_annotation(self.ann_file_path(ann_filename))
        return self

    def add_orphan_category_annotation(self, ann_filename: str) -> "DatasetBuilder":
        mut.add_orphan_category_annotation(self.ann_file_path(ann_filename))
        return self

    def mismatch_category_labels(self, ann_filename: str) -> "DatasetBuilder":
        mut.mismatch_category_labels(self.ann_file_path(ann_filename))
        return self

    # --- Image file mutations ---

    def remove_image_file(
        self, ann_filename: str, image_index: int = 0
    ) -> Optional[str]:
        return mut.remove_image_file(
            self.images_dir, self.ann_file_path(ann_filename), image_index
        )

    def add_image_without_annotation(
        self, ann_filename: str, fake_name: str = "orphan_image.jpg"
    ) -> "DatasetBuilder":
        mut.add_image_without_annotation(
            self.images_dir, self.ann_file_path(ann_filename), fake_name
        )
        return self

    # --- Cross-split mutations ---

    def inject_split_leakage(
        self, ann_file_1: str, ann_file_2: str, count: int = 5
    ) -> "DatasetBuilder":
        mut.inject_split_leakage(
            self.ann_file_path(ann_file_1),
            self.ann_file_path(ann_file_2),
            count,
        )
        return self

    # --- Thumbnail helpers ---

    def ensure_no_thumbnail(self) -> "DatasetBuilder":
        thumb = osp.join(self._path, "thumbnail.jpg")
        if osp.exists(thumb):
            os.remove(thumb)
        return self

    def create_thumbnail(self) -> "DatasetBuilder":
        thumb = osp.join(self._path, "thumbnail.jpg")
        try:
            from PIL import Image
            img = Image.new("RGB", (100, 100), color="blue")
            img.save(thumb, "JPEG")
        except ImportError:
            with open(thumb, "wb") as f:
                f.write(b"\xff\xd8\xff\xe0" + b"\x00" * 100)
        return self

    def remove_all_images(self) -> "DatasetBuilder":
        if osp.isdir(self.images_dir):
            shutil.rmtree(self.images_dir)
            os.makedirs(self.images_dir)
        return self
