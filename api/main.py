import io
import sys
import os
import json
from datetime import datetime, UTC

import torch
import torchvision.transforms as transforms
from fastapi import FastAPI, File, UploadFile, HTTPException
from PIL import Image
from confluent_kafka import Producer

sys.path.insert(0, os.path.dirname(__file__))
from model_loader import load_model

app = FastAPI(title="Pneumonia Classifier API")

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
MODEL = load_model(device=DEVICE)
CONFIDENCE_THRESHOLD = 0.7

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CLINICAL_ALERTS_TOPIC = "clinical-alerts"

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

transform = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.ToTensor(),
    transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
])


def delivery_report(err, msg):
    if err is not None:
        print(f"Kafka delivery failed: {err}")


@app.get("/health")
def health():
    return {"status": "ok", "device": DEVICE}


@app.post("/predict")
async def predict(file: UploadFile = File(...)):
    if not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="File must be an image")

    try:
        contents = await file.read()
        image = Image.open(io.BytesIO(contents)).convert("RGB")
    except Exception:
        raise HTTPException(status_code=400, detail="Could not read image file. File may be corrupt.")

    input_tensor = transform(image).unsqueeze(0).to(DEVICE)

    with torch.no_grad():
        output = MODEL(input_tensor).squeeze(1)
        probability = torch.sigmoid(output).item()

    prediction = {
        "filename": file.filename,
        "pneumonia_probability": round(probability, 4),
        "prediction": "Pneumonia" if probability > 0.5 else "Normal/Other",
        "high_confidence_positive": probability > CONFIDENCE_THRESHOLD,
    }

    if prediction["high_confidence_positive"]:
        alert = {
            "filename": file.filename,
            "pneumonia_probability": prediction["pneumonia_probability"],
            "alert_timestamp": datetime.now(UTC).isoformat(),
        }
        try:
            producer.produce(
                CLINICAL_ALERTS_TOPIC,
                value=json.dumps(alert).encode("utf-8"),
                callback=delivery_report,
            )
            producer.flush(timeout=2.0)
        except Exception as e:
            print(f"Kafka alert publish failed (non-fatal): {e}")

    return prediction