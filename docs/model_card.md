# Model Card — Pneumonia Classifier v1

## Overview

Binary classifier predicting pneumonia presence (Lung Opacity) from chest X-ray images, built on EfficientNet-B0 transfer learning. Trained as part of an end-to-end medallion lakehouse pipeline — the model's primary purpose is to validate that the Gold layer produces genuinely model-ready data, not to achieve state-of-the-art clinical performance.

## Training Data

- **Source:** RSNA Pneumonia Detection Challenge dataset (Kaggle)
- **Training set size:** 3,000 of 15,659 available images, due to local disk constraints in the development environment (see main README, "Scoping Decisions"). The pipeline architecture supports the full dataset; this was an environment limitation, not a design limitation.
- **Split methodology:** Patient-aware stratified split (70% train / 15% val / 15% test), assigned at the patient level to prevent data leakage — critical since RSNA contains multiple opacity annotations per patient.
- **Class balance:** ~3.86:1 negative:positive ratio in the training set, addressed via `pos_weight` in the loss function.

## Model Architecture

- **Backbone:** EfficientNet-B0, pretrained on ImageNet (via `timm`)
- **Training approach:** Two-phase transfer learning
  - Phase 1 (3 epochs): backbone frozen, only classifier head trained
  - Phase 2 (8 epochs): full network fine-tuned, cosine annealing LR schedule (1e-4 → 0)
- **Loss function:** `BCEWithLogitsLoss` with `pos_weight=3.86` to address class imbalance

## Performance

| Metric | Validation | Test |
|---|---|---|
| AUROC | 0.7546 | **0.7813** |
| Loss | 2.21 | 1.97 |

**Confusion matrix (test set, threshold=0.5):**

| | Predicted Normal | Predicted Pneumonia |
|---|---|---|
| **Actual Normal** | 280 | 56 |
| **Actual Pneumonia** | 45 | 42 |

| Class | Precision | Recall | F1 |
|---|---|---|---|
| Normal/Other | 0.86 | 0.83 | 0.85 |
| Pneumonia | 0.43 | 0.48 | 0.45 |

## Known Limitations

- **Training set size is the primary limiting factor.** The model was trained on 19% of the available dataset. Train loss approaching near-zero (0.017) while validation loss climbed (1.99 → 2.41) during fine-tuning indicates overfitting consistent with a small training set, not a flawed training procedure — patient-aware splitting and `pos_weight` were both correctly applied.
- **Pneumonia recall (0.48) and precision (0.43) are below what would be acceptable for clinical deployment.** This model is a pipeline validation artifact, not a clinical screening tool. Roughly half of true pneumonia cases would be missed, and over half of positive predictions would be false alarms.
- **Demographic bias:** Not formally evaluated for performance disparities across the `patient_sex` and `patient_age_bucket` fields available in the Gold layer. A production system would require this analysis before deployment.
- **No external validation:** Evaluated only on RSNA's held-out test split, from the same source distribution as training data. Performance on X-rays from different hospitals, scanners, or patient populations is unknown.

## Path to Improvement

Training on the full 15,659-image dataset (the architecture already supports this) would be the single highest-impact change. Additional improvements worth exploring: data augmentation (rotation, contrast jitter) to artificially expand the effective training set, and threshold tuning beyond the default 0.5 cutoff to trade precision for recall depending on clinical priority.

## Intended Use

This model is part of a portfolio data engineering project demonstrating an end-to-end medallion lakehouse architecture. It is **not** intended for clinical use, diagnosis, or any decision affecting patient care.