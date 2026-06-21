import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_predict_with_valid_image():
    image_dir = os.path.join(os.path.dirname(__file__), "..", "silver", "images_processed")
    sample_files = os.listdir(image_dir)
    sample_path = os.path.join(image_dir, sample_files[0])

    with open(sample_path, "rb") as f:
        response = client.post("/predict", files={"file": (sample_files[0], f, "image/png")})

    assert response.status_code == 200
    body = response.json()
    assert "pneumonia_probability" in body
    assert 0.0 <= body["pneumonia_probability"] <= 1.0
    assert body["prediction"] in ["Pneumonia", "Normal/Other"]
    assert isinstance(body["high_confidence_positive"], bool)


def test_predict_with_corrupt_file():
    corrupt_content = b"this is not a valid image file"
    response = client.post(
        "/predict",
        files={"file": ("corrupt.png", corrupt_content, "image/png")},
    )
    assert response.status_code == 400
    assert "detail" in response.json()


def test_predict_with_non_image_content_type():
    response = client.post(
        "/predict",
        files={"file": ("test.txt", b"hello world", "text/plain")},
    )
    assert response.status_code == 400