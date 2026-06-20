import os
import pydicom
from collections import Counter

DICOM_DIR = "data/raw/stage_2_train_images"
SAMPLE_SIZE = 200

files = sorted(os.listdir(DICOM_DIR))[:SAMPLE_SIZE]

view_positions = Counter()
modalities = Counter()
sexes = Counter()

for filename in files:
    path = os.path.join(DICOM_DIR, filename)
    ds = pydicom.dcmread(path, stop_before_pixels=True)
    view_positions[ds.get("ViewPosition", "MISSING")] += 1
    modalities[ds.get("Modality", "MISSING")] += 1
    sexes[ds.get("PatientSex", "MISSING")] += 1

print("View Positions:", view_positions)
print("Modalities:", modalities)
print("Sexes:", sexes)