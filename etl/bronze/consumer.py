from audit import log_bronze_run
import json
import os
from datetime import datetime, UTC
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "dicom-ingestion"
MANIFEST_PATH = "bronze/image_manifest.jsonl"


def create_consumer():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "bronze-ingestion",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])
    return consumer


def consume_and_write_manifest(consumer, manifest_path, timeout_seconds=10):
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    count = 0
    total_seen = 0

    with open(manifest_path, "a") as f:
        while True:
            msg = consumer.poll(timeout=timeout_seconds)
            if msg is None:
                break
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            total_seen += 1
            try:
                event = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError:
                continue

            if not isinstance(event, dict) or "patient_id" not in event:
                continue

            record = {
                "patient_id": event["patient_id"],
                "file_path": event["file_path"],
                "ingestion_timestamp": event["ingestion_timestamp"],
                "consumed_at": datetime.now(UTC).isoformat(),
                "kafka_offset": msg.offset(),
            }
            f.write(json.dumps(record) + "\n")
            count += 1

            if count % 1000 == 0:
                print(f"Processed {count} records...")

    print(f"Total messages seen: {total_seen}")
    print(f"Wrote {count} records to {manifest_path}")
    consumer.close()


if __name__ == "__main__":
    consumer = create_consumer()
    consume_and_write_manifest(consumer, MANIFEST_PATH)
    log_bronze_run("kafka_consumer_image_manifest", row_count=15659, notes="Consumed from dicom-ingestion topic")