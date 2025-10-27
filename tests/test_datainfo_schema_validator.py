import json
import unittest
import os.path as osp
from pydantic import ValidationError
from modelcat.connector.utils.schemas.datainfo import DatasetInfo, DatasetInfos
from modelcat.connector.utils.misc import _get_dataset_path


def minimal_splits_dict():
    return {
        "train": {
            "name": "train",
            "dataset_name": "coco_train.json",
            "num_examples": 100,
            "num_bytes": 1234,
        },
        "validation": {
            "name": "validation",
            "dataset_name": "coco_val.json",
            "num_examples": 20,
            "num_bytes": 234,
        },
        "test": {
            "name": "test",
            "dataset_name": "coco_test.json",
            "num_examples": 20,
            "num_bytes": 345,
        },
    }


class TestTaskTemplateWithDicts(unittest.TestCase):
    def test_classification_minimal_dict(self):
        payload = {
            "task_templates": [{"task": "classification", "labels": ["cat", "dog"]}],
            "splits": minimal_splits_dict(),
        }
        di = DatasetInfo.parse_obj(payload)
        self.assertEqual(di.task_templates[0].task, "classification")
        self.assertEqual(di.task_templates[0].labels, ["cat", "dog"])

    def test_detection_minimal_dict(self):
        payload = {
            "task_templates": [{"task": "detection", "labels": ["person"]}],
            "splits": minimal_splits_dict(),
        }
        di = DatasetInfo.parse_obj(payload)
        self.assertEqual(di.task_templates[0].task, "detection")

    def test_keypoints_requires_num_keypoints(self):
        payload = {
            "task_templates": [
                {"task": "keypoints", "labels": ["person"]}  # missing num_keypoints
            ],
            "splits": minimal_splits_dict(),
        }
        with self.assertRaises(ValidationError) as ctx:
            DatasetInfo.parse_obj(payload)
        self.assertIn("num_keypoints", str(ctx.exception))

    def test_keypoints_num_keypoints_must_be_positive_int(self):
        for bad in [0, -1, 3.14, "8", None]:
            with self.subTest(bad=bad):
                payload = {
                    "task_templates": [
                        {
                            "task": "keypoints",
                            "labels": ["person"],
                            "num_keypoints": bad,
                        }
                    ],
                    "splits": minimal_splits_dict(),
                }
                with self.assertRaises(ValidationError):
                    DatasetInfo.parse_obj(payload)

        ok_payload = {
            "task_templates": [
                {"task": "keypoints", "labels": ["person"], "num_keypoints": 8}
            ],
            "splits": minimal_splits_dict(),
        }
        di = DatasetInfo.parse_obj(ok_payload)
        self.assertEqual(di.task_templates[0].num_keypoints, 8)

    def test_annotations_subset_enforced_when_provided(self):
        # classification requires {"category_id"}
        payload = {
            "task_templates": [
                {"task": "classification", "labels": ["x"], "annotations": ["id"]}
            ],
            "splits": minimal_splits_dict(),
        }
        with self.assertRaises(ValidationError):
            DatasetInfo.parse_obj(payload)

        # detection requires {"category_id", "bbox"}
        payload = {
            "task_templates": [
                {"task": "detection", "labels": ["x"], "annotations": ["category_id"]}
            ],
            "splits": minimal_splits_dict(),
        }
        with self.assertRaises(ValidationError):
            DatasetInfo.parse_obj(payload)

        # keypoints requires {"category_id", "bbox", "keypoints"}
        payload = {
            "task_templates": [
                {
                    "task": "keypoints",
                    "labels": ["x"],
                    "num_keypoints": 5,
                    "annotations": ["bbox", "category_id"],
                }
            ],
            "splits": minimal_splits_dict(),
        }
        with self.assertRaises(ValidationError):
            DatasetInfo.parse_obj(payload)

        # ok with required subset + extras
        payload = {
            "task_templates": [
                {
                    "task": "keypoints",
                    "labels": ["x"],
                    "num_keypoints": 5,
                    "annotations": ["category_id", "bbox", "keypoints", "id", "area"],
                }
            ],
            "splits": minimal_splits_dict(),
        }
        di = DatasetInfo.parse_obj(payload)
        self.assertEqual(di.task_templates[0].task, "keypoints")

    def test_task_literal_invalid(self):
        payload = {
            "task_templates": [
                {"task": "segmentation", "labels": ["x"]}  # invalid Literal
            ],
            "splits": minimal_splits_dict(),
        }
        with self.assertRaises(ValidationError):
            DatasetInfo.parse_obj(payload)

    def test_exactly_one_task_template_required(self):
        # none
        payload = {"task_templates": [], "splits": minimal_splits_dict()}
        with self.assertRaises(ValidationError):
            DatasetInfo.parse_obj(payload)

        # two
        payload = {
            "task_templates": [
                {"task": "detection", "labels": ["a"]},
                {"task": "classification", "labels": ["b"]},
            ],
            "splits": minimal_splits_dict(),
        }
        with self.assertRaises(ValidationError):
            DatasetInfo.parse_obj(payload)


class TestSplitsWithDicts(unittest.TestCase):
    def test_three_required_splits_present(self):
        payload = {
            "task_templates": [{"task": "classification", "labels": ["x"]}],
            "splits": minimal_splits_dict(),
        }
        di = DatasetInfo.parse_obj(payload)
        self.assertIn("train", di.splits)
        self.assertIn("validation", di.splits)
        self.assertIn("test", di.splits)

    def test_missing_split_keys_raise(self):
        payload = {
            "task_templates": [{"task": "classification", "labels": ["x"]}],
            "splits": {  # missing 'test'
                "train": {"name": "train", "dataset_name": "a.json"},
                "validation": {"name": "validation", "dataset_name": "b.json"},
            },
        }
        with self.assertRaises(ValidationError) as ctx:
            DatasetInfo.parse_obj(payload)
        self.assertIn("splits must include", str(ctx.exception))

    def test_splits_must_be_mapping(self):
        payload = {
            "task_templates": [{"task": "classification", "labels": ["x"]}],
            "splits": None,
        }
        with self.assertRaises(ValidationError):
            DatasetInfo.parse_obj(payload)

        payload["splits"] = []  # not a mapping
        with self.assertRaises(ValidationError):
            DatasetInfo.parse_obj(payload)

    def test_splitinfo_required_fields_via_dict(self):
        # split missing required 'dataset_name'
        payload = {
            "task_templates": [{"task": "classification", "labels": ["x"]}],
            "splits": {
                "train": {"name": "train"},  # <-- missing dataset_name
                "validation": {"name": "validation", "dataset_name": "b.json"},
                "test": {"name": "test", "dataset_name": "c.json"},
            },
        }
        with self.assertRaises(ValidationError):
            DatasetInfo.parse_obj(payload)


class TestFeaturesWithDicts(unittest.TestCase):
    def test_features_optional_absent(self):
        payload = {
            "task_templates": [{"task": "classification", "labels": ["x"]}],
            "splits": minimal_splits_dict(),
            # no "features" key at all
        }
        di = DatasetInfo.parse_obj(payload)
        self.assertIsNone(di.features)

    def test_features_basic_image_sequence_dict(self):
        payload = {
            "task_templates": [{"task": "detection", "labels": ["person"]}],
            "splits": minimal_splits_dict(),
            "features": {
                "image": {"_type": "Image", "id": None},
                "labels": {"_type": "Sequence", "id": None},
            },
        }
        di = DatasetInfo.parse_obj(payload)
        self.assertEqual(di.features.image.type, "Image")
        self.assertEqual(di.features.labels.type, "Sequence")

    def test_features_nested_objects_like_samples(self):
        payload = {
            "task_templates": [{"task": "detection", "labels": ["person"]}],
            "splits": minimal_splits_dict(),
            "features": {
                "image": {"_type": "Image"},
                "labels": {"_type": "Sequence"},
                "image/filename": {"_type": "Text"},
                "objects": {
                    "_type": "Sequence",
                    "objects_bbox": {"_type": "BBoxFeature"},
                    "objects_label": {
                        "_type": "ClassLabel",
                        "num_classes": 1,
                        "names": ["person"],
                    },
                },
            },
        }
        di = DatasetInfo.parse_obj(payload)
        self.assertEqual(di.features.image.type, "Image")


class TestDatasetInfosWithDicts(unittest.TestCase):
    def mk_di_dict(self, task="classification"):
        return {
            "task_templates": [{"task": task, "labels": ["x"]}],
            "splits": minimal_splits_dict(),
        }

    def test_parse_top_level_mapping(self):
        data = {
            "cats_dogs": self.mk_di_dict("classification"),
            "people_det": self.mk_di_dict("detection"),
        }
        infos = DatasetInfos.parse_obj(data)
        self.assertIn("cats_dogs", infos.__root__)
        self.assertEqual(infos["people_det"].task_templates[0].task, "detection")

    def test_parse_from_json_string(self):
        data = {"my_ds": self.mk_di_dict("classification")}
        json_str = json.dumps(data)
        infos = DatasetInfos.parse_raw(json_str)
        self.assertIn("my_ds", infos.__root__)

    def test_datasetinfos_value_must_be_datasetinfo_shape(self):
        bad = {
            "ok_ds": self.mk_di_dict("classification"),
            "bad_ds": {"not": "a DatasetInfo"},
        }
        with self.assertRaises(ValidationError):
            DatasetInfos.parse_obj(bad)

    def test_load_sample_dataset_info_classification(self):
        ds_path = osp.join(_get_dataset_path(), "classification_sample")
        datainfo_path = osp.join(ds_path, "dataset_infos.json")
        with open(datainfo_path, "r") as f:
            di_dict = json.load(f)
        di = DatasetInfos.parse_obj(di_dict)["classification_sample"]
        self.assertEqual(di.task_templates[0].task, "classification")
        self.assertIn("train", di.splits)
        self.assertIn("validation", di.splits)
        self.assertIn("test", di.splits)

    def test_load_sample_dataset_info_object_detection(self):
        ds_path = osp.join(_get_dataset_path(), "objectdetection_sample")
        datainfo_path = osp.join(ds_path, "dataset_infos.json")
        with open(datainfo_path, "r") as f:
            di_dict = json.load(f)
        di = DatasetInfos.parse_obj(di_dict)["objectdetection_sample"]
        self.assertEqual(di.task_templates[0].task, "detection")
        self.assertIn("train", di.splits)
        self.assertIn("validation", di.splits)
        self.assertIn("test", di.splits)

    def test_load_sample_dataset_info_keypoint_detection(self):
        ds_path = osp.join(_get_dataset_path(), "keypoints_sample")
        datainfo_path = osp.join(ds_path, "dataset_infos.json")
        with open(datainfo_path, "r") as f:
            di_dict = json.load(f)
        di = DatasetInfos.parse_obj(di_dict)["keypoints_sample"]
        self.assertEqual(di.task_templates[0].task, "keypoints")
        self.assertIn("train", di.splits)
        self.assertIn("validation", di.splits)
        self.assertIn("test", di.splits)


if __name__ == "__main__":
    unittest.main(verbosity=2)
