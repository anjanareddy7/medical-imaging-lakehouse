import hashlib
import os
import pydicom


def anonymize_patient_id(patient_id):
    """Hash the patient ID so the original identifier is never stored in Silver."""
    return hashlib.sha256(patient_id.encode("utf-8")).hexdigest()[:16]


def bucket_age(age_str):
    """Convert exact age into a 10-year bucket to reduce re-identification risk."""
    try:
        age = int(age_str)
    except (ValueError, TypeError):
        return "UNKNOWN"
    bucket_start = (age // 10) * 10
    return f"{bucket_start}-{bucket_start + 9}"


def parse_dicom_file(file_path):
    """Extract structured, de-identified metadata from a single DICOM file."""
    ds = pydicom.dcmread(file_path, stop_before_pixels=True)

    raw_patient_id = ds.get("PatientID", "")
    original_filename = os.path.basename(file_path).replace(".dcm", "")

    record = {
        "patient_id_hash": anonymize_patient_id(raw_patient_id) if raw_patient_id else None,
        "original_filename": original_filename,  # matches RSNA labels, not PHI itself
        "modality": ds.get("Modality", None),
        "view_position": ds.get("ViewPosition", None),
        "body_part_examined": ds.get("BodyPartExamined", None),
        "patient_sex": ds.get("PatientSex", None),
        "patient_age_bucket": bucket_age(ds.get("PatientAge", "").rstrip("Y") if ds.get("PatientAge") else None),
        "rows": ds.get("Rows", None),
        "columns": ds.get("Columns", None),
        "study_instance_uid": ds.get("StudyInstanceUID", None),
        "series_instance_uid": ds.get("SeriesInstanceUID", None),
        "file_path": file_path,
    }
    return record


if __name__ == "__main__":
    sample_file = "data/raw/stage_2_train_images/0004cfab-14fd-4e49-80ba-63a80b6bddd6.dcm"
    record = parse_dicom_file(sample_file)
    for k, v in record.items():
        print(f"{k}: {v}")