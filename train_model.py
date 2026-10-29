"""
train_model.py – One-time training of the IsolationForest on synthetic data.

Run this BEFORE starting the consumer.  Output: models/isolation_forest.pkl

The training set is a large synthetic sample drawn from the same distribution
as the producer, plus injected anomalies so the model learns the boundary.
"""
import os
import pickle
import logging
import numpy as np
from sklearn.ensemble import IsolationForest
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [TRAIN] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

RANDOM_STATE = 42
N_NORMAL     = 5_000
N_ANOMALY    = 500


def build_training_data() -> np.ndarray:
    rng = np.random.default_rng(RANDOM_STATE)

    # Normal latencies: Gaussian around 120 ms, floored at 10 ms
    normal = rng.normal(loc=120, scale=30, size=N_NORMAL)
    normal = np.clip(normal, 10, None)

    # Anomalous latencies: uniform 1 500 – 6 000 ms (matches producer spikes)
    anomaly = rng.uniform(1_500, 6_000, size=N_ANOMALY)

    X = np.concatenate([normal, anomaly]).reshape(-1, 1)
    logger.info("Training set: %d normal + %d anomaly samples", N_NORMAL, N_ANOMALY)
    return X


def train_and_save():
    X = build_training_data()

    model = IsolationForest(
        n_estimators=200,
        contamination=N_ANOMALY / (N_NORMAL + N_ANOMALY),
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    model.fit(X)

    os.makedirs(os.path.dirname(config.ANOMALY_MODEL_PATH), exist_ok=True)
    with open(config.ANOMALY_MODEL_PATH, "wb") as fh:
        pickle.dump(model, fh)

    # Quick sanity check
    test_normal  = model.predict([[100.0], [130.0], [80.0]])
    test_anomaly = model.predict([[3000.0], [5000.0]])
    logger.info("Sanity – normal predictions (expect +1): %s", test_normal)
    logger.info("Sanity – anomaly predictions (expect -1): %s", test_anomaly)
    logger.info("Model saved → %s", config.ANOMALY_MODEL_PATH)


if __name__ == "__main__":
    train_and_save()
