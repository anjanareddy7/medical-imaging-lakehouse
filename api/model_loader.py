import torch
import timm

MODEL_PATH = "models/pneumonia_classifier_v1.pt"


def load_model(device="cpu"):
    """Load the trained EfficientNet-B0 pneumonia classifier."""
    model = timm.create_model("efficientnet_b0", pretrained=False, num_classes=1)
    state_dict = torch.load(MODEL_PATH, map_location=device)
    model.load_state_dict(state_dict)
    model.to(device)
    model.eval()
    return model