import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "etl", "silver"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "etl", "bronze"))

from dicom_parser import anonymize_patient_id, bucket_age


def test_anonymize_patient_id_is_consistent():
    """Same input should always produce the same hash."""
    id1 = anonymize_patient_id("patient-123")
    id2 = anonymize_patient_id("patient-123")
    assert id1 == id2


def test_anonymize_patient_id_differs_for_different_inputs():
    """Different patient IDs should hash to different values."""
    id1 = anonymize_patient_id("patient-123")
    id2 = anonymize_patient_id("patient-456")
    assert id1 != id2


def test_anonymize_patient_id_does_not_contain_original():
    """The hash should never contain the original ID as a substring."""
    original = "patient-123"
    hashed = anonymize_patient_id(original)
    assert original not in hashed


def test_bucket_age_buckets_correctly():
    assert bucket_age("51") == "50-59"
    assert bucket_age("38") == "30-39"
    assert bucket_age("9") == "0-9"
    assert bucket_age("100") == "100-109"


def test_bucket_age_handles_invalid_input():
    assert bucket_age("") == "UNKNOWN"
    assert bucket_age(None) == "UNKNOWN"
    assert bucket_age("abc") == "UNKNOWN"