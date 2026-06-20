import json
import os
from datetime import datetime, UTC

AUDIT_LOG_PATH = "bronze/audit_log.jsonl"


def log_bronze_run(job_name, row_count, schema=None, notes=None):
    os.makedirs(os.path.dirname(AUDIT_LOG_PATH), exist_ok=True)

    record = {
        "job_name": job_name,
        "run_timestamp": datetime.now(UTC).isoformat(),
        "row_count": row_count,
        "schema": schema,
        "notes": notes,
    }

    with open(AUDIT_LOG_PATH, "a") as f:
        f.write(json.dumps(record) + "\n")

    print(f"[AUDIT] {job_name}: {row_count} rows at {record['run_timestamp']}")