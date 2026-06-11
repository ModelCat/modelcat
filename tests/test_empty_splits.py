import json
import os
import os.path as osp
import shutil
import tempfile
import unittest

from modelcat.connector.validate import DatasetValidator
from modelcat.connector.utils.misc import _get_dataset_path


def _empty_split_file(ann_path: str) -> None:
    """Clear images and annotations of a COCO split file (keep categories)."""
    with open(ann_path) as f:
        coco = json.load(f)
    coco["images"] = []
    coco["annotations"] = []
    with open(ann_path, "w") as f:
        json.dump(coco, f)


def _copy_sample(sample: str, tmp: str) -> str:
    ds_path = osp.join(_get_dataset_path(), sample)
    shutil.copytree(ds_path, tmp, dirs_exist_ok=True)
    return tmp


class TestCheckEmptySplitsUnit(unittest.TestCase):
    """Directly exercise DatasetValidator.check_empty_splits in isolation."""

    ANN_FILES = ["coco_train.json", "coco_validation.json", "coco_test.json"]
    SPLITS = ["train", "validation", "test"]

    def _make_validator(self, tmp: str) -> DatasetValidator:
        _copy_sample("classification_sample", tmp)
        return DatasetValidator(tmp, tmp)

    def test_non_empty_splits_produce_no_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            dsv = self._make_validator(tmp)
            msgs = dsv.check_empty_splits(self.ANN_FILES, self.SPLITS)
            self.assertEqual(msgs, [])

    def test_empty_split_produces_single_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            dsv = self._make_validator(tmp)
            _empty_split_file(osp.join(tmp, "annotations", "coco_test.json"))
            msgs = dsv.check_empty_splits(self.ANN_FILES, self.SPLITS)
            self.assertEqual(len(msgs), 1)
            self.assertEqual(msgs[0]["type"], "error")
            self.assertIn("empty", msgs[0]["message"].lower())
            self.assertIn("test", msgs[0]["message"])

    def test_each_split_flagged_when_empty(self):
        for split_file, split_name in zip(self.ANN_FILES, self.SPLITS):
            with self.subTest(split=split_name):
                with tempfile.TemporaryDirectory() as tmp:
                    dsv = self._make_validator(tmp)
                    _empty_split_file(osp.join(tmp, "annotations", split_file))
                    msgs = dsv.check_empty_splits(self.ANN_FILES, self.SPLITS)
                    errors = [m for m in msgs if m["type"] == "error"]
                    self.assertEqual(len(errors), 1)
                    self.assertIn(split_name, errors[0]["message"])

    def test_multiple_empty_splits_each_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            dsv = self._make_validator(tmp)
            _empty_split_file(osp.join(tmp, "annotations", "coco_test.json"))
            _empty_split_file(osp.join(tmp, "annotations", "coco_validation.json"))
            msgs = dsv.check_empty_splits(self.ANN_FILES, self.SPLITS)
            errors = [m for m in msgs if m["type"] == "error"]
            self.assertEqual(len(errors), 2)

    def test_missing_file_is_skipped(self):
        # A missing annotation file is reported by validate_coco_file, not here.
        with tempfile.TemporaryDirectory() as tmp:
            dsv = self._make_validator(tmp)
            os.remove(osp.join(tmp, "annotations", "coco_test.json"))
            msgs = dsv.check_empty_splits(["coco_test.json"], ["test"])
            self.assertEqual(msgs, [])

    def test_malformed_file_is_skipped(self):
        # A malformed annotation file is reported by validate_coco_file, not here.
        with tempfile.TemporaryDirectory() as tmp:
            dsv = self._make_validator(tmp)
            with open(osp.join(tmp, "annotations", "coco_test.json"), "w") as f:
                f.write("{not valid json")
            msgs = dsv.check_empty_splits(["coco_test.json"], ["test"])
            self.assertEqual(msgs, [])

    def test_missing_images_section_is_skipped(self):
        # A missing "images" section is a param_check error, not an empty-split error.
        with tempfile.TemporaryDirectory() as tmp:
            dsv = self._make_validator(tmp)
            ann_path = osp.join(tmp, "annotations", "coco_test.json")
            with open(ann_path) as f:
                coco = json.load(f)
            del coco["images"]
            with open(ann_path, "w") as f:
                json.dump(coco, f)
            msgs = dsv.check_empty_splits(["coco_test.json"], ["test"])
            self.assertEqual(msgs, [])


class TestEmptySplitsValidateFlow(unittest.TestCase):
    """Empty splits surface as errors through validate_dataset and block signing."""

    def _validate_with_empty_split(self, sample: str, split_file: str):
        with tempfile.TemporaryDirectory() as tmp:
            _copy_sample(sample, tmp)
            _empty_split_file(osp.join(tmp, "annotations", split_file))
            dsv = DatasetValidator(tmp, tmp)
            while True:
                msgs, restart = dsv.validate_dataset()
                if not restart:
                    break
            # An error must block signing.
            dsv.create_validation_mark()
            with open(osp.join(tmp, "dataset_validator_log.txt")) as f:
                log_text = f.read()
            return msgs, log_text

    def _assert_empty_error_and_unsigned(self, msgs, log_text):
        errors = [m for m in msgs if m["type"] == "error"]
        self.assertTrue(
            any("empty" in m["message"].lower() for m in errors),
            f"expected an empty-split error, got: {errors}",
        )
        self.assertNotIn("Validation passed and signed", log_text)

    def test_empty_test_split_classification(self):
        msgs, log_text = self._validate_with_empty_split(
            "classification_sample", "coco_test.json"
        )
        self._assert_empty_error_and_unsigned(msgs, log_text)

    def test_empty_train_split_detection(self):
        msgs, log_text = self._validate_with_empty_split(
            "objectdetection_sample", "coco_train.json"
        )
        self._assert_empty_error_and_unsigned(msgs, log_text)

    def test_empty_validation_split_keypoints(self):
        msgs, log_text = self._validate_with_empty_split(
            "keypoints_sample", "coco_validation.json"
        )
        self._assert_empty_error_and_unsigned(msgs, log_text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
