import json
import os
import time
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "dicom-ingestion"
DICOM_DIR = "data/raw/stage_2_train_images"


def create_producer():
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def publish_dicom_events(producer, dicom_dir, limit=None):
    files = [f for f in os.listdir(dicom_dir) if f.endswith(".dcm")]
    if limit:
        files = files[:limit]

    print(f"Found {len(files)} DICOM files. Publishing events...")

    for i, filename in enumerate(files):
        event = {
            "patient_id": filename.replace(".dcm", ""),
            "file_path": os.path.join(dicom_dir, filename),
            "ingestion_timestamp": time.time(),
        }
        producer.produce(
            TOPIC,
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)

        if (i + 1) % 500 == 0:
            print(f"Published {i + 1}/{len(files)} events")

    producer.flush()
    print("Done publishing all events.")


if __name__ == "__main__":
    producer = create_producer()
    publish_dicom_events(producer, DICOM_DIR, limit=None)