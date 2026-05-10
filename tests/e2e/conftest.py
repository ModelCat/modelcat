import os
import os.path as osp

import pytest

from tests.e2e.models.cli_runner import CLIRunner
from tests.e2e.models.dataset_builder import DatasetBuilder

REPO_ROOT = osp.abspath(osp.join(osp.dirname(__file__), "..", ".."))
SAMPLE_DATASETS_DIR = osp.join(REPO_ROOT, "sample_datasets")

CLASSIFICATION_SAMPLE = osp.join(SAMPLE_DATASETS_DIR, "classification_sample")
OBJECT_DETECTION_SAMPLE = osp.join(SAMPLE_DATASETS_DIR, "objectdetection_sample")
KEYPOINTS_SAMPLE = osp.join(SAMPLE_DATASETS_DIR, "keypoints_sample")


@pytest.fixture
def cli():
    """Provides a CLIRunner instance for invoking modelcat_validate."""
    return CLIRunner()


@pytest.fixture
def classification_ds(tmp_path):
    """Provides a mutable copy of the classification sample dataset."""
    return DatasetBuilder(CLASSIFICATION_SAMPLE, str(tmp_path))


@pytest.fixture
def detection_ds(tmp_path):
    """Provides a mutable copy of the object detection sample dataset."""
    return DatasetBuilder(OBJECT_DETECTION_SAMPLE, str(tmp_path))


@pytest.fixture
def keypoints_ds(tmp_path):
    """Provides a mutable copy of the keypoints sample dataset."""
    return DatasetBuilder(KEYPOINTS_SAMPLE, str(tmp_path))
