import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, col

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bronze"))
from audit import log_bronze_run

GOLD_MODEL_READY_PATH = "gold/model_ready"
DQ_RESULTS_PATH = "silver/dq_results"
OUTPUT_PATH = "gold/analytics_summary"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("GoldAnalyticsSummary")
        .config("spark.jars", "jars/postgresql-42.7.3.jar,jars/delta-spark_4.1_2.13-4.1.0.jar,jars/delta-storage-4.1.0.jar")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def build_analytics_summary():
    spark = create_spark_session()

    model_ready_df = spark.read.format("delta").load(GOLD_MODEL_READY_PATH)

    # Case counts by modality and view position
    case_counts_df = (
        model_ready_df.groupBy("modality", "view_position", "label")
        .agg(count("*").alias("case_count"))
    )

    print("=== Case counts by modality/view/label ===")
    case_counts_df.show()

    # Label distribution by sex and age bucket
    demographic_df = (
        model_ready_df.groupBy("patient_sex", "patient_age_bucket", "label")
        .agg(count("*").alias("case_count"))
    )

    print("=== Demographic breakdown ===")
    demographic_df.show(20)

    # DQ pass rate summary
    dq_df = spark.read.format("delta").load(DQ_RESULTS_PATH)
    dq_summary_df = (
        dq_df.groupBy("check_name")
        .agg(
            count("*").alias("total_checks"),
            avg(col("passed").cast("int")).alias("pass_rate"),
        )
    )

    print("=== DQ pass rate summary ===")
    dq_summary_df.show()

    case_counts_df.write.format("delta").mode("overwrite").save(f"{OUTPUT_PATH}/case_counts")
    demographic_df.write.format("delta").mode("overwrite").save(f"{OUTPUT_PATH}/demographics")
    dq_summary_df.write.format("delta").mode("overwrite").save(f"{OUTPUT_PATH}/dq_summary")

    total_rows = case_counts_df.count() + demographic_df.count() + dq_summary_df.count()
    log_bronze_run(
        "gold_analytics_summary",
        row_count=total_rows,
        notes="Built case_counts, demographics, dq_summary tables",
    )

    print(f"Wrote analytics tables to {OUTPUT_PATH}/")
    spark.stop()


if __name__ == "__main__":
    build_analytics_summary()