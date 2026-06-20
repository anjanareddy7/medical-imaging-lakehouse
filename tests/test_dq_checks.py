import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "etl", "silver"))

from dq_checks import check_duplicate_images


def test_duplicate_detection_finds_no_duplicates_in_unique_files(tmp_path):
    """Two files with different content should not be flagged as duplicates."""
    file1 = tmp_path / "image1.png"
    file2 = tmp_path / "image2.png"
    file1.write_bytes(b"unique content one")
    file2.write_bytes(b"unique content two")

    results = check_duplicate_images(str(tmp_path), sample_size=10)
    assert results[0]["passed"] is True
    assert results[0]["failing_rows"] == 0


def test_duplicate_detection_finds_real_duplicates(tmp_path):
    """Two files with identical content should be flagged."""
    file1 = tmp_path / "image1.png"
    file2 = tmp_path / "image2.png"
    file1.write_bytes(b"identical content")
    file2.write_bytes(b"identical content")

    results = check_duplicate_images(str(tmp_path), sample_size=10)
    assert results[0]["passed"] is False
    assert results[0]["failing_rows"] == 1


def test_duplicate_detection_respects_sample_size(tmp_path):
    """Only sample_size files should be checked, not the whole directory."""
    for i in range(5):
        (tmp_path / f"image{i}.png").write_bytes(f"content {i}".encode())

    results = check_duplicate_images(str(tmp_path), sample_size=3)
    assert results[0]["total_rows"] == 3