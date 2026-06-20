import hashlib
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bronze"))
from audit import log_bronze_run

REQUIRED_FIELDS = [
    "patient_id_hash", "original_filename", "modality",
    "view_position", "patient_sex", "rows", "columns",
]

DQ_OUTPUT_PATH = "silver/dq_results"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverDQChecks")
        .config("spark.jars", "jars/postgresql-42.7.3.jar,jars/delta-spark_4.1_2.13-4.1.0.jar,jars/delta-storage-4.1.0.jar")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def check_schema_completeness(df, required_fields):
    """For each required field, count how many rows are missing it."""
    results = []
    total_rows = df.count()

    for field in required_fields:
        null_count = df.filter(col(field).isNull()).count()
        results.append({
            "check_name": "schema_completeness",
            "field": field,
            "total_rows": total_rows,
            "failing_rows": null_count,
            "passed": null_count == 0,
        })
    return results


def check_referential_integrity(metadata_df, labels_path):
    """Every patient_id in the labels CSV should have a matching DICOM file."""
    import psycopg2
    import pandas as pd

    conn = psycopg2.connect(
        host="localhost", port=5432, dbname="hospital_db",
        user="hospital", password="hospital123",
    )
    labels_pdf = pd.read_sql("SELECT DISTINCT patient_id FROM raw_class_info", conn)
    conn.close()

    metadata_filenames = set(
        row["original_filename"] for row in metadata_df.select("original_filename").collect()
    )
    label_ids = set(labels_pdf["patient_id"])

    missing_images = label_ids - metadata_filenames
    total_labels = len(label_ids)

    return [{
        "check_name": "referential_integrity",
        "field": "patient_id_to_image",
        "total_rows": total_labels,
        "failing_rows": len(missing_images),
        "passed": len(missing_images) == 0,
    }]


def check_duplicate_images(image_dir, sample_size=3000):
    """Hash each image's bytes; flag any duplicate hashes."""
    hashes = {}
    duplicates = []

    files = [f for f in os.listdir(image_dir) if f.endswith(".png")][:sample_size]

    for filename in files:
        path = os.path.join(image_dir, filename)
        with open(path, "rb") as f:
            file_hash = hashlib.md5(f.read()).hexdigest()

        if file_hash in hashes:
            duplicates.append((filename, hashes[file_hash]))
        else:
            hashes[file_hash] = filename

    return [{
        "check_name": "duplicate_detection",
        "field": "image_pixel_hash",
        "total_rows": len(files),
        "failing_rows": len(duplicates),
        "passed": len(duplicates) == 0,
    }]


if __name__ == "__main__":
    spark = create_spark_session()
    metadata_df = spark.read.format("delta").load("silver/dicom_metadata")

    all_results = []
    all_results.extend(check_schema_completeness(metadata_df, REQUIRED_FIELDS))
    all_results.extend(check_referential_integrity(metadata_df, "data/raw/stage_2_detailed_class_info.csv"))
    all_results.extend(check_duplicate_images("silver/images_processed"))

    print("\n=== DQ Results ===")
    for r in all_results:
        status = "PASS" if r["passed"] else "FAIL"
        print(f"[{status}] {r['check_name']} ({r['field']}): {r['failing_rows']}/{r['total_rows']} failing")

    results_df = spark.createDataFrame(all_results)
    results_df.write.format("delta").mode("overwrite").save(DQ_OUTPUT_PATH)

    log_bronze_run(
        "silver_dq_checks",
        row_count=len(all_results),
        notes=f"Ran {len(all_results)} DQ checks",
    )

    spark.stop()