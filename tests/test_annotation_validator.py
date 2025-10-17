import json
import copy
import unittest
import os.path as osp
from pydantic import ValidationError
from modelcat.connector.utils.schemas.annotation import CocoDataset
from modelcat.connector.utils.misc import _get_dataset_path


def valid_base_dataset():
    """
    Returns a minimal valid dataset dict with:
    - 1 license
    - 1 category (no keypoints)
    - 1 image
    - 1 annotation (no bbox/keypoints)
    """
    return {
        "info": {"year": "2025", "version": "1.0"},
        "licenses": [{"id": 1, "name": "MIT"}],
        "categories": [{"id": 1, "name": "cat", "supercategory": "animal"}],
        "images": [{"id": 1, "file_name": "img_0001.jpg", "license": 1}],
        "annotations": [
            {
                "id": 1,
                "image_id": 1,
                "category_id": 1,
                "bbox": [],
                "segmentation": None,
                "iscrowd": None,
                "area": None,
                "keypoints": None,
                "num_keypoints": None,
            }
        ],
    }


def valid_kp_dataset(K=4):
    """
    Minimal valid dataset for a keypointed category with K keypoints.
    """
    ds = {
        "info": {"year": "2025"},
        "licenses": [{"id": 1}],
        "categories": [
            {
                "id": 1,
                "name": "person",
                "supercategory": "person",
                "keypoints": [f"k{i}" for i in range(K)],
            }
        ],
        "images": [
            {"id": 1, "file_name": "img.jpg", "license": 1, "width": 100, "height": 100}
        ],
        "annotations": [
            {
                "id": 1,
                "image_id": 1,
                "category_id": 1,
                # 3*K numbers: (x,y,v) repeating; all visible (v=2) at (0,0) is allowed by your validator
                "keypoints": sum(([0, 0, 2] for _ in range(K)), []),
                "num_keypoints": K,
                "bbox": [],
                "segmentation": None,
                "iscrowd": 0,
                "area": None,
            }
        ],
    }
    return ds


class TestCocoSchemaFailures(unittest.TestCase):
    # category validator failures
    def test_category_keypoints_not_list(self):
        data = valid_base_dataset()
        data["categories"][0]["keypoints"] = "not-a-list"
        with self.assertRaises(ValidationError):
            CocoDataset.parse_obj(data)

    def test_category_keypoints_not_strings(self):
        data = valid_base_dataset()
        data["categories"][0]["keypoints"] = [1, 2]
        with self.assertRaises(ValidationError):
            CocoDataset.parse_obj(data)

    def test_category_keypoints_empty_strings(self):
        data = valid_base_dataset()
        data["categories"][0]["keypoints"] = ["ok", "  "]
        with self.assertRaises(ValidationError):
            CocoDataset.parse_obj(data)

    def test_skeleton_not_list(self):
        data = valid_base_dataset()
        data["categories"][0]["keypoints"] = ["a", "b"]
        data["categories"][0]["skeleton"] = "not-a-list"
        with self.assertRaises(ValidationError):
            CocoDataset.parse_obj(data)

    def test_skeleton_bad_pair_shape(self):
        data = valid_base_dataset()
        data["categories"][0]["keypoints"] = ["a", "b"]
        data["categories"][0]["skeleton"] = [[1, 2], [3]]  # second pair invalid
        with self.assertRaises(ValidationError):
            CocoDataset.parse_obj(data)

    def test_skeleton_non_int_indices(self):
        data = valid_base_dataset()
        data["categories"][0]["keypoints"] = ["a", "b"]
        data["categories"][0]["skeleton"] = [["1", 2]]
        with self.assertRaises(ValidationError):
            CocoDataset.parse_obj(data)

    def test_skeleton_zero_or_negative(self):
        data = valid_base_dataset()
        data["categories"][0]["keypoints"] = ["a", "b"]
        data["categories"][0]["skeleton"] = [[0, 2]]
        with self.assertRaises(ValidationError):
            CocoDataset.parse_obj(data)

    def test_skeleton_out_of_range(self):
        data = valid_base_dataset()
        data["categories"][0]["keypoints"] = ["a", "b"]
        data["categories"][0]["skeleton"] = [[1, 3]]  # only 2 keypoints
        with self.assertRaises(ValidationError):
            CocoDataset.parse_obj(data)

    # image failures
    def test_image_missing_license_reference(self):
        data = valid_base_dataset()
        data["images"][0]["license"] = 999
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn("references unknown license id", str(ctx.exception))

    def test_image_empty_filename(self):
        data = valid_base_dataset()
        data["images"][0]["file_name"] = "   "
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn("non-empty 'file_name'", str(ctx.exception))

    def test_duplicate_image_filenames(self):
        data = valid_base_dataset()
        data["images"].append({"id": 2, "file_name": "img_0001.jpg", "license": 1})
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn("Duplicate image file_name", str(ctx.exception))

    # annotation field validator failures
    def test_bbox_wrong_length(self):
        data = valid_base_dataset()
        data["annotations"][0]["bbox"] = [1, 2, 3]  # not 4
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn("bbox must have 4 elements", str(ctx.exception))

    def test_bbox_non_numeric(self):
        data = valid_base_dataset()
        data["annotations"][0]["bbox"] = [0, "y", 10, 10]
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn("value is not a valid", str(ctx.exception))

    def test_bbox_negative(self):
        data = valid_base_dataset()
        data["annotations"][0]["bbox"] = [0, 0, -5, 10]
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn("bbox values must be non-negative", str(ctx.exception))

    def test_keypoints_not_list_or_bad_mod(self):
        data = valid_base_dataset()
        data["categories"][0]["keypoints"] = ["a", "b"]
        # root validator will require ann.keypoints present; give wrong shape to trigger field validator
        data["annotations"][0]["keypoints"] = [0, 0, 2, 1]  # len%3 != 0
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn("keypoints must be a flat list", str(ctx.exception))

    def test_keypoints_bad_visibility_value(self):
        data = valid_kp_dataset(K=2)
        data["annotations"][0]["keypoints"][2] = 9  # v must be 0,1,2
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn("visibility values must be 0,1,2", str(ctx.exception))

    def test_keypoints_v0_requires_zero_coords(self):
        data = valid_kp_dataset(K=2)
        # set first triplet to v=0 but coords not zero
        data["annotations"][0]["keypoints"][:3] = [5, 5, 0]
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn("When visibility v=0", str(ctx.exception))

    def test_keypoints_visible_coords_must_be_non_negative(self):
        data = valid_kp_dataset(K=2)
        # make one visible kp negative
        data["annotations"][0]["keypoints"][0:3] = [-1, 0, 2]
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn(
            "Visible keypoints must have non-negative coordinates", str(ctx.exception)
        )

    def test_iscrowd_invalid(self):
        data = valid_base_dataset()
        data["annotations"][0]["iscrowd"] = 2  # not 0/1
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn("iscrowd must be integer 0 or 1", str(ctx.exception))

    # root validator failures: uniqueness
    def test_duplicate_license_ids(self):
        data = valid_base_dataset()
        data["licenses"].append({"id": 1, "name": "dup"})
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn("Duplicate license id", str(ctx.exception))

    def test_duplicate_category_ids(self):
        data = valid_base_dataset()
        data["categories"].append({"id": 1, "name": "dog", "supercategory": "animal"})
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn("Category ids must be contiguous", str(ctx.exception))

    def test_duplicate_image_ids(self):
        data = valid_base_dataset()
        data["images"].append({"id": 1, "file_name": "img_0002.jpg", "license": 1})
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn("Duplicate image id", str(ctx.exception))

    def test_duplicate_annotation_ids(self):
        data = valid_base_dataset()
        ann = copy.deepcopy(data["annotations"][0])
        ann["id"] = 1
        ann["image_id"] = 1
        ann["category_id"] = 1
        data["annotations"].append(ann)
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn("Duplicate annotation id", str(ctx.exception))

    # root validator failures: category name uniqueness
    def test_duplicate_category_names(self):
        data = valid_base_dataset()
        data["categories"] = [
            {"id": 1, "name": "cat", "supercategory": "animal"},
            {"id": 2, "name": "cat", "supercategory": "animal"},
        ]
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn("Duplicate category name", str(ctx.exception))

    # root validator failures: category ids must be contiguous starting at 1
    def test_category_ids_not_contiguous_from_1(self):
        data = valid_base_dataset()
        data["categories"] = [
            {"id": 1, "name": "a", "supercategory": "animal"},
            {"id": 3, "name": "b", "supercategory": "animal"},
        ]
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn(
            "Category ids must be contiguous starting at 1", str(ctx.exception)
        )

    # root validator failures: invalid foreign keys
    def test_annotation_unknown_image_id(self):
        data = valid_base_dataset()
        data["annotations"][0]["image_id"] = 999
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn("references unknown image_id", str(ctx.exception))

    def test_annotation_unknown_category_id(self):
        data = valid_base_dataset()
        data["annotations"][0]["category_id"] = 999
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn("references unknown category_id", str(ctx.exception))

    # root validator failures: keypoints rules when category has keypoints
    def test_kp_category_requires_ann_keypoints_presence(self):
        data = valid_kp_dataset(K=3)
        # remove keypoints to trigger "must include 'keypoints'"
        data["annotations"][0]["keypoints"] = None
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn("must include 'keypoints'", str(ctx.exception))

    def test_kp_category_keypoints_length_mismatch(self):
        data = valid_kp_dataset(K=3)
        # have only 2*3 numbers instead of 3*3
        data["annotations"][0]["keypoints"] = [0, 0, 2, 0, 0, 2]
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn("keypoints length must be", str(ctx.exception))

    def test_kp_category_num_keypoints_mismatch(self):
        data = valid_kp_dataset(K=3)
        # make only 2 visible (set one triplet to v=0 with (0,0))
        data["annotations"][0]["keypoints"] = [0, 0, 2, 0, 0, 2, 0, 0, 0]
        data["annotations"][0]["num_keypoints"] = 3
        with self.assertRaises(ValidationError) as ctx:
            CocoDataset.parse_obj(data)
        self.assertIn("num_keypoints=", str(ctx.exception))


def _load_coco(path: str) -> CocoDataset:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return CocoDataset.parse_obj(data)


class TestCocoDatasetSuccess(unittest.TestCase):
    def _assert_basic_coco_integrity(self, coco: CocoDataset):
        # sanity: the model parsed, and lists are present
        self.assertIsInstance(coco.categories, list)
        self.assertIsInstance(coco.images, list)
        self.assertIsInstance(coco.annotations, list)
        self.assertGreaterEqual(len(coco.categories), 1)
        self.assertGreater(len(coco.images), 0)

        # every annotation image_id & category_id must be known (the model already enforces this;
        # these checks document expectations and give clearer assertion failures if something changes)
        cat_ids = {c.id for c in coco.categories}
        img_ids = {im.id for im in coco.images}
        for ann in coco.annotations:
            self.assertIn(
                ann.category_id,
                cat_ids,
                f"unknown category_id {ann.category_id} in ann {ann.id}",
            )
            self.assertIn(
                ann.image_id,
                img_ids,
                f"unknown image_id {ann.image_id} in ann {ann.id}",
            )

        # segmentation should always be present (validator defaults to [[]])
        for ann in coco.annotations:
            self.assertIsNotNone(ann.segmentation)

        # file_name must be present and non-empty
        for im in coco.images:
            self.assertTrue(isinstance(im.file_name, str) and im.file_name.strip())

    def _assert_classification_rules(self, coco: CocoDataset):
        # in sample classification split, bbox is empty for all annotations
        for ann in coco.annotations:
            self.assertTrue(ann.bbox == [] or ann.bbox is None)

    def _assert_detection_rules(self, coco: CocoDataset):
        # for detection, each annotation must have a 4-element bbox (model enforces this when provided;
        # these asserts document the intended dataset convention)
        for ann in coco.annotations:
            self.assertIsInstance(ann.bbox, list)
            # some datasets allow crowd-only anns without bbox; sample shows bbox for all.
            self.assertEqual(len(ann.bbox), 4, f"ann {ann.id} missing 4-element bbox")

    def _assert_keypoints_rules(self, coco: CocoDataset):
        # map category_id -> expected K (or None if no keypoints)
        cat_to_k = {
            c.id: (len(c.keypoints) if c.keypoints else None) for c in coco.categories
        }
        for ann in coco.annotations:
            K = cat_to_k.get(ann.category_id)
            if K:
                self.assertIsInstance(
                    ann.keypoints, list, f"ann {ann.id} must include keypoints"
                )
                self.assertEqual(
                    len(ann.keypoints),
                    3 * K,
                    f"ann {ann.id} keypoints length must be 3*K",
                )
                # if num_keypoints present, it must match count of v>0
                if ann.num_keypoints is not None:
                    visible = sum(
                        1
                        for i in range(2, len(ann.keypoints), 3)
                        if int(ann.keypoints[i]) > 0
                    )
                    self.assertEqual(
                        ann.num_keypoints,
                        visible,
                        f"ann {ann.id} num_keypoints mismatch (got {ann.num_keypoints}, vis {visible})",
                    )

    # classification sample
    def test_load_sample_coco_classification(self):
        ds_path = osp.join(_get_dataset_path(), "classification_sample")
        for split in ("coco_train.json", "coco_validation.json", "coco_test.json"):
            with self.subTest(split=split):
                coco = _load_coco(osp.join(ds_path, "annotations", split))
                self._assert_basic_coco_integrity(coco)
                self._assert_classification_rules(coco)

    # object detection sample
    def test_load_sample_coco_detection(self):
        ds_path = osp.join(_get_dataset_path(), "objectdetection_sample")
        for split in ("coco_train.json", "coco_validation.json", "coco_test.json"):
            with self.subTest(split=split):
                coco = _load_coco(osp.join(ds_path, "annotations", split))
                self._assert_basic_coco_integrity(coco)
                self._assert_detection_rules(coco)

    # keypoints detection sample
    def test_load_sample_coco_keypoints(self):
        ds_path = osp.join(_get_dataset_path(), "keypoints_sample")
        for split in ("coco_train.json", "coco_validation.json", "coco_test.json"):
            with self.subTest(split=split):
                coco = _load_coco(osp.join(ds_path, "annotations", split))
                self._assert_basic_coco_integrity(coco)
                self._assert_keypoints_rules(coco)


if __name__ == "__main__":
    unittest.main(verbosity=2)
