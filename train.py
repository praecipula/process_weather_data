"""
train.py
========
Main entry point for training the weather prediction model.
"""

import os
import datetime
import numpy as np
from lib.db import session
from lib.area_config import get_area
from lib.input_vector import SequenceBuilder
from lib.keras_model import WeatherLSTM

def train_area(area_key: str):
    print(f"--- Starting Training for Area: {area_key} ---")
    
    # 1. Load Area Configuration
    area = get_area(area_key)
    
    # 2. Prepare Data
    print("Gathering data from database...")
    builder = SequenceBuilder(area, session)
    try:
        X, y_max, y_min, metas = builder.make_arrays()
    except ValueError as e:
        print(f"Error: {e}")
        return

    print(f"Dataset prepared: {X.shape[0]} samples")
    print(f"Input shape: {X.shape[1:]}")
    print(f"Target anomaly buckets: {y_max.shape[1]}")

    # 3. Initialize Model
    weather_model = WeatherLSTM(area)
    weather_model.summary()

    # 4. Train
    # We add callbacks to log progress to a file and save the best model weights.
    from tensorflow.keras.callbacks import CSVLogger, ModelCheckpoint
    
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    csv_path = os.path.join(log_dir, f"train_{area_key}.csv")
    
    model_dir = "models"
    os.makedirs(model_dir, exist_ok=True)
    checkpoint_path = os.path.join(model_dir, f"best_weather_{area_key}.keras")

    callbacks = [
        CSVLogger(csv_path, append=False),
        ModelCheckpoint(checkpoint_path, save_best_only=True, monitor="val_loss")
    ]

    print(f"\nStarting training loop. Logging to {csv_path}...")
    history = weather_model.model.fit(
        X, 
        {"max_temp_anomaly": y_max, "min_temp_anomaly": y_min},
        epochs=100, # Increased for a real run
        batch_size=16, 
        validation_split=0.2,
        callbacks=callbacks
    )

    print(f"\nTraining complete. Model log saved to: {csv_path}")

if __name__ == "__main__":
    # For now, let's just train for the San Francisco Bay Area
    train_area("sfbay")
