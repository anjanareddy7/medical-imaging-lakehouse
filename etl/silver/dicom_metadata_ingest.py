import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bronze"))
from dicom_parser import parse_dicom_file
from audit import log_bronze_run

DICOM_DIR = "data/raw/stage_2_train_images"
OUTPUT_PATH = "silver/dicom_metadata"

SCHEMA = StructType([
    StructField("patient_id_hash", StringType(), True),
    StructField("original_filename", StringType(), True),
    StructField("modality", StringType(), True),
    StructField("view_position", StringType(), True),
    StructField("body_part_examined", StringType(), True),
    StructField("patient_sex", StringType(), True),
    StructField("patient_age_bucket", StringType(), True),
    StructField("rows", IntegerType(), True),
    StructField("columns", IntegerType(), True),
    StructField("study_instance_uid", StringType(), True),
    StructField("series_instance_uid", StringType(), True),
    StructField("file_path", StringType(), True),
])


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverDicomMetadataIngest")
        .config("spark.jars", "jars/postgresql-42.7.3.jar,jars/delta-spark_4.1_2.13-4.1.0.jar,jars/delta-storage-4.1.0.jar")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def parse_all_dicoms(dicom_dir, limit=None):
    files = [f for f in os.listdir(dicom_dir) if f.endswith(".dcm")]
    if limit:
        files = files[:limit]

    records = []
    errors = []

    for i, filename in enumerate(files):
        path = os.path.join(dicom_dir, filename)
        try:
            record = parse_dicom_file(path)
            records.append(record)
        except Exception as e:
            errors.append({"file": filename, "error": str(e)})

        if (i + 1) % 1000 == 0:
            print(f"Parsed {i + 1}/{len(files)} files")

    print(f"Done. {len(records)} succeeded, {len(errors)} failed.")
    if errors:
        print("Sample errors:", errors[:5])

    return records, errors


if __name__ == "__main__":
    records, errors = parse_all_dicoms(DICOM_DIR, limit=None)

    spark = create_spark_session()
    df = spark.createDataFrame(records, schema=SCHEMA)

    df.write.format("delta").mode("overwrite").save(OUTPUT_PATH)

    row_count = df.count()
    log_bronze_run(
        "silver_dicom_metadata_ingest",
        row_count=row_count,
        schema=df.schema.simpleString(),
        notes=f"Parsed {len(records)} DICOM files, {len(errors)} failed",
    )

    print(f"Wrote {row_count} rows to {OUTPUT_PATH}")
    spark.stop()