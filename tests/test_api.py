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
    from PIL import Image
    import io

    # Generate a synthetic grayscale image so this test doesn't depend on the dataset being present
    img = Image.new("L", (224, 224), color=128)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)

    response = client.post("/predict", files={"file": ("synthetic.png", buf, "image/png")})

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