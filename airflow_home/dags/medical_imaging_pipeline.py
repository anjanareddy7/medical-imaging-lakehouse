import sys
import os
from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

PROJECT_ROOT = "/workspaces/medical-imaging-lakehouse"
sys.path.insert(0, os.path.join(PROJECT_ROOT, "etl", "bronze"))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "etl", "silver"))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "etl", "gold"))


def run_bronze_metadata_ingest():
    import subprocess
    result = subprocess.run(
        ["poetry", "run", "python", "etl/bronze/metadata_ingest.py"],
        cwd=PROJECT_ROOT, capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError("Bronze metadata ingest failed")


def run_silver_dq_checks():
    import subprocess
    result = subprocess.run(
        ["poetry", "run", "python", "etl/silver/dq_checks.py"],
        cwd=PROJECT_ROOT, capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError("Silver DQ checks failed")


def run_gold_model_ready():
    import subprocess
    result = subprocess.run(
        ["poetry", "run", "python", "etl/gold/model_ready.py"],
        cwd=PROJECT_ROOT, capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError("Gold model_ready failed")


def run_gold_analytics():
    import subprocess
    result = subprocess.run(
        ["poetry", "run", "python", "etl/gold/analytics_summary.py"],
        cwd=PROJECT_ROOT, capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError("Gold analytics failed")


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="medical_imaging_pipeline",
    description="Bronze -> Silver DQ -> Gold model-ready -> Gold analytics",
    schedule=None,  # manual trigger only for now
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["medical-imaging", "lakehouse"],
) as dag:

    bronze_ingest = PythonOperator(
        task_id="bronze_metadata_ingest",
        python_callable=run_bronze_metadata_ingest,
    )

    silver_dq = PythonOperator(
        task_id="silver_dq_checks",
        python_callable=run_silver_dq_checks,
    )

    gold_model_ready = PythonOperator(
        task_id="gold_model_ready",
        python_callable=run_gold_model_ready,
    )

    gold_analytics = PythonOperator(
        task_id="gold_analytics_summary",
        python_callable=run_gold_analytics,
    )

    bronze_ingest >> silver_dq >> gold_model_ready >> gold_analytics