"""
Signature and output tests.

Validates that successful validation produces a signature hash
and that failures do not produce one.
"""
import os.path as osp

import pytest


@pytest.mark.e2e
class TestSignatureAndOutput:

    def test_valid_dataset_produces_signature(self, cli, classification_ds):
        """J1: Valid dataset should produce a 64-char hex signature."""
        result = cli.validate(classification_ds.path)
        assert result.passed
        assert result.signature is not None
        assert len(result.signature) == 64
        assert result.stdout_contains("Validation passed and signed")

    def test_invalid_dataset_no_signature(self, cli, classification_ds):
        """J2: Invalid dataset should not produce a signature."""
        classification_ds.remove_file("dataset_infos.json")
        result = cli.validate(classification_ds.path)
        assert not result.passed
        assert result.signature is None
        assert result.stdout_contains("error") or result.stdout_contains("not validated")

    def test_log_file_created(self, cli, classification_ds):
        """J3: Validation should create dataset_validator_log.txt with details."""
        result = cli.validate(classification_ds.path)
        log_path = osp.join(classification_ds.path, "dataset_validator_log.txt")
        assert osp.exists(log_path)

    def test_summary_section_present(self, cli, classification_ds):
        """Stdout should contain Messages and Summary sections."""
        result = cli.validate(classification_ds.path)
        assert "messages" in result.stdout.lower()
        assert "summary" in result.stdout.lower()
