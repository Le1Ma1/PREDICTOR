import json
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score, average_precision_score, brier_score_loss

def basic_scores(y_true, proba):
    proba = np.clip(proba, 1e-6, 1-1e-6)
    return {
        "AUC": float(roc_auc_score(y_true, proba)),
        "PR_AUC": float(average_precision_score(y_true, proba)),
        "Brier": float(brier_score_loss(y_true, proba)),
    }

def save_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
