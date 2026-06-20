import json
import os
from datetime import datetime, UTC
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "dicom-ingestion"
MANIFEST_PATH = "bronze/image_manifest.jsonl"

def create_consumer():
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v.decode("utf-8"),
        consumer_timeout_ms=10000,  # stop after 10s of no new messages
    )

def consume_and_write_manifest(consumer, manifest_path):
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    count = 0

    with open(manifest_path, "a") as f:
        for message in consumer:
            try:
                event = json.loads(message.value)
            except json.JSONDecodeError:
                continue  # skip old plain-text test messages

            if not isinstance(event, dict) or "patient_id" not in event:
                continue

            record = {
                "patient_id": event["patient_id"],
                "file_path": event["file_path"],
                "ingestion_timestamp": event["ingestion_timestamp"],
                "consumed_at": datetime.now(UTC).isoformat(),
                "kafka_offset": message.offset,
            }
            f.write(json.dumps(record) + "\n")
            count += 1

    print(f"Wrote {count} records to {manifest_path}")

if __name__ == "__main__":
    consumer = create_consumer()
    consume_and_write_manifest(consumer, MANIFEST_PATH)