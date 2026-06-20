import os
import cv2
import numpy as np
import pydicom

DICOM_DIR = "data/raw/stage_2_train_images"
OUTPUT_DIR = "silver/images_processed"
TARGET_SIZE = (224, 224)


def preprocess_dicom_image(file_path, target_size=TARGET_SIZE):
    """Decode, normalize, and resize a DICOM image. Returns a numpy array or raises on failure."""
    ds = pydicom.dcmread(file_path)
    pixel_array = ds.pixel_array

    # Normalize to 0-255 regardless of original bit depth
    pixel_array = pixel_array.astype(np.float32)
    pixel_min, pixel_max = pixel_array.min(), pixel_array.max()
    if pixel_max > pixel_min:
        pixel_array = (pixel_array - pixel_min) / (pixel_max - pixel_min) * 255.0
    pixel_array = pixel_array.astype(np.uint8)

    resized = cv2.resize(pixel_array, target_size, interpolation=cv2.INTER_AREA)
    return resized


def process_all_images(dicom_dir, output_dir, limit=None):
    os.makedirs(output_dir, exist_ok=True)
    files = [f for f in os.listdir(dicom_dir) if f.endswith(".dcm")]
    if limit:
        files = files[:limit]

    success_count = 0
    failures = []

    for i, filename in enumerate(files):
        path = os.path.join(dicom_dir, filename)
        try:
            img = preprocess_dicom_image(path)
            out_path = os.path.join(output_dir, filename.replace(".dcm", ".png"))
            cv2.imwrite(out_path, img)
            success_count += 1
        except Exception as e:
            failures.append({"file": filename, "error": str(e)})

        if (i + 1) % 500 == 0:
            print(f"Processed {i + 1}/{len(files)}")

    print(f"Done. {success_count} succeeded, {len(failures)} failed.")
    return success_count, failures


if __name__ == "__main__":
    # Processing 3000 of 15,659 images due to local disk constraints.
    # Pipeline is designed to scale to the full dataset given more storage.
    success, failures = process_all_images(DICOM_DIR, OUTPUT_DIR, limit=3000)
    if failures:
        print("Sample failures:", failures[:5])