import json
import os
import pandas as pd
from confluent_kafka import Consumer, KafkaException
import time

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
AUDIT_LOG_PATH = os.path.join(PROJECT_ROOT, "bronze", "audit_log.jsonl")
DQ_RESULTS_PATH = os.path.join(PROJECT_ROOT, "silver", "dq_results")
ANALYTICS_PATH = os.path.join(PROJECT_ROOT, "gold", "analytics_summary")


def load_audit_log():
    """Load pipeline run history from the audit log."""
    if not os.path.exists(AUDIT_LOG_PATH):
        return pd.DataFrame()

    records = []
    with open(AUDIT_LOG_PATH, "r") as f:
        for line in f:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    df = pd.DataFrame(records)
    if not df.empty:
        df["run_timestamp"] = pd.to_datetime(df["run_timestamp"])
        df = df.sort_values("run_timestamp", ascending=False)
    return df


def load_dq_results():
    """Load DQ check results via Spark since it's a Delta table."""
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("DashboardDQRead")
        .config("spark.jars", f"{PROJECT_ROOT}/jars/postgresql-42.7.3.jar,{PROJECT_ROOT}/jars/delta-spark_4.1_2.13-4.1.0.jar,{PROJECT_ROOT}/jars/delta-storage-4.1.0.jar")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    df = spark.read.format("delta").load(DQ_RESULTS_PATH).toPandas()
    spark.stop()
    return df


def load_analytics_summary():
    """Load Gold analytics tables."""
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("DashboardAnalyticsRead")
        .config("spark.jars", f"{PROJECT_ROOT}/jars/postgresql-42.7.3.jar,{PROJECT_ROOT}/jars/delta-spark_4.1_2.13-4.1.0.jar,{PROJECT_ROOT}/jars/delta-storage-4.1.0.jar")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    case_counts = spark.read.format("delta").load(f"{ANALYTICS_PATH}/case_counts").toPandas()
    demographics = spark.read.format("delta").load(f"{ANALYTICS_PATH}/demographics").toPandas()
    spark.stop()
    return case_counts, demographics


def fetch_recent_alerts(max_messages=20, timeout_seconds=5):
    """Pull recent messages from the clinical-alerts Kafka topic."""
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": f"dashboard-reader-{int(time.time())}",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(["clinical-alerts"])

    alerts = []
    start = time.time()
    while time.time() - start < timeout_seconds and len(alerts) < max_messages:
        msg = consumer.poll(timeout=0.5)
        if msg is None:
            continue
        if msg.error():
            continue
        try:
            alerts.append(json.loads(msg.value().decode("utf-8")))
        except json.JSONDecodeError:
            continue

    consumer.close()
    return alerts