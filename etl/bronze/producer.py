import json
import os
import time
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "dicom-ingestion"
DICOM_DIR = "data/raw/stage_2_train_images"

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def publish_dicom_events(producer, dicom_dir, limit=None):
    """Publish a Kafka event for each DICOM file found, simulating arrival."""
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
        producer.send(TOPIC, value=event)

        if (i + 1) % 100 == 0:
            print(f"Published {i + 1}/{len(files)} events")

    producer.flush()
    print("Done publishing all events.")

if __name__ == "__main__":
    producer = create_producer()
    # Start with a small limit to test before running on all 15,659 files
    publish_dicom_events(producer, DICOM_DIR, limit=50)