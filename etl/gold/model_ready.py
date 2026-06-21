import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
import random

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bronze"))
from audit import log_bronze_run

SILVER_METADATA_PATH = "silver/dicom_metadata"
GOLD_OUTPUT_PATH = "gold/model_ready"

POSTGRES_URL = "jdbc:postgresql://localhost:5432/hospital_db"
POSTGRES_PROPERTIES = {
    "user": "hospital",
    "password": "hospital123",
    "driver": "org.postgresql.Driver",
}

TRAIN_RATIO = 0.70
VAL_RATIO = 0.15
# remaining 0.15 goes to test


def create_spark_session():
    return (
        SparkSession.builder
        .appName("GoldModelReady")
        .config("spark.jars", "jars/postgresql-42.7.3.jar,jars/delta-spark_4.1_2.13-4.1.0.jar,jars/delta-storage-4.1.0.jar")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def assign_patient_splits(patient_ids, seed=42):
    """Shuffle patient IDs deterministically and assign train/val/test."""
    ids = sorted(patient_ids)  # sort first for determinism before shuffling
    random.Random(seed).shuffle(ids)

    n = len(ids)
    train_end = int(n * TRAIN_RATIO)
    val_end = train_end + int(n * VAL_RATIO)

    split_map = {}
    for i, pid in enumerate(ids):
        if i < train_end:
            split_map[pid] = "train"
        elif i < val_end:
            split_map[pid] = "val"
        else:
            split_map[pid] = "test"
    return split_map


def build_gold_model_ready():
    spark = create_spark_session()

    # Read Silver metadata (one row per parsed DICOM file)
    metadata_df = spark.read.format("delta").load(SILVER_METADATA_PATH)

    # Read labels from Postgres (one row per opacity detection; patient_id may repeat)
    labels_df = spark.read.jdbc(
        url=POSTGRES_URL, table="raw_class_info", properties=POSTGRES_PROPERTIES
    )

    # Binary label: Lung Opacity = 1, everything else = 0
    labels_df = labels_df.withColumn(
        "label", when(col("class") == "Lung Opacity", lit(1)).otherwise(lit(0))
    )

    # Collapse to one row per patient (a patient is positive if ANY of their rows is Lung Opacity)
    patient_labels_df = (
        labels_df.groupBy("patient_id")
        .agg({"label": "max"})
        .withColumnRenamed("max(label)", "label")
    )

    # Join with Silver metadata on original_filename == patient_id
    joined_df = metadata_df.join(
        patient_labels_df,
        metadata_df.original_filename == patient_labels_df.patient_id,
        "inner",
    )

    print(f"Joined rows (images with labels): {joined_df.count()}")

    # Patient-aware split
    patient_ids = [row["patient_id"] for row in patient_labels_df.select("patient_id").distinct().collect()]
    split_map = assign_patient_splits(patient_ids)

    print(f"Total unique patients: {len(patient_ids)}")
    train_count = sum(1 for v in split_map.values() if v == "train")
    val_count = sum(1 for v in split_map.values() if v == "val")
    test_count = sum(1 for v in split_map.values() if v == "test")
    print(f"Train: {train_count}, Val: {val_count}, Test: {test_count}")

    # Broadcast the split map as a Spark DataFrame and join
    split_rows = [(pid, split) for pid, split in split_map.items()]
    split_df = spark.createDataFrame(split_rows, ["patient_id", "split"])

    final_df = joined_df.join(split_df, on="patient_id", how="inner").select(
        "patient_id",
        "original_filename",
        "label",
        "split",
        "modality",
        "view_position",
        "patient_sex",
        "patient_age_bucket",
        "rows",
        "columns",
        "file_path",
    )

    final_df.write.format("delta").mode("overwrite").partitionBy("split").save(GOLD_OUTPUT_PATH)

    final_count = final_df.count()
    print(f"Wrote {final_count} rows to {GOLD_OUTPUT_PATH}")

    log_bronze_run(
        "gold_model_ready",
        row_count=final_count,
        schema=final_df.schema.simpleString(),
        notes=f"Patient-aware split: train={train_count}, val={val_count}, test={test_count} patients",
    )

    spark.stop()


if __name__ == "__main__":
    build_gold_model_ready()