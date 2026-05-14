"""
train.py
========
Staged Training Protocol with Rich-enhanced logging.
Phase 1: Full Discovery.
Phase 2: Context Freeze (Live Head Focus).
"""

import os
import datetime
import numpy as np
import tensorflow as tf
from lib.db import session
from lib.area_config import get_area
from lib.input_vector import SequenceBuilder
from lib.keras_model import WeatherLSTM
from tensorflow.keras.callbacks import CSVLogger, ModelCheckpoint, EarlyStopping, Callback
from rich.console import Console
from rich.panel import Panel

console = Console()

class EarlyStoppingProgress(Callback):
    def __init__(self, patience=10, prefix=""):
        super().__init__()
        self.patience = patience
        self.best_loss = float('inf')
        self.wait = 0
        self.prefix = prefix

    def on_epoch_end(self, epoch, logs=None):
        current_val_loss = logs.get('val_loss')
        if current_val_loss is None: return
        if current_val_loss < self.best_loss:
            self.best_loss = current_val_loss
            self.wait = 0
        else:
            self.wait += 1
            color = "yellow" if self.wait < self.patience / 2 else "red"
            msg = f"[{color}]No improvement for {self.wait}/{self.patience} epochs. Best val_loss: {self.best_loss:.4f}[/]"
            console.print(Panel(msg, title=f"{self.prefix} Early Stopping Monitor", expand=False))

def train_area(area_key: str):
    console.print(f"[bold blue]--- Starting STAGED Training for Area: {area_key} ---[/]")
    area = get_area(area_key)
    builder = SequenceBuilder(area, session)
    try:
        X_live, X_context, y_max, y_min, metas = builder.make_arrays()
    except ValueError as e:
        console.print(f"[bold red]Error:[/] {e}")
        return

    console.print(f"[green]Dataset prepared:[/] {X_live.shape[0]} samples")
    weather_model = WeatherLSTM(area)
    
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    csv_path = os.path.join(log_dir, f"train_{area_key}.csv")
    
    model_dir = "models"
    os.makedirs(model_dir, exist_ok=True)
    checkpoint_path = os.path.join(model_dir, f"best_weather_{area_key}.keras")

    # =========================================================================
    # PHASE 1: JOINT DISCOVERY
    # =========================================================================
    console.rule("[bold green]PHASE 1: Full Discovery[/]")
    patience_1 = 15
    callbacks_1 = [
        CSVLogger(csv_path, append=False),
        ModelCheckpoint(checkpoint_path, save_best_only=True, monitor="val_loss"),
        EarlyStopping(monitor="val_loss", patience=patience_1, restore_best_weights=True),
        EarlyStoppingProgress(patience=patience_1, prefix="Phase 1")
    ]

    weather_model.model.fit(
        {"main_input": X_live, "context_input": X_context}, 
        {"max_temp_anomaly": y_max, "min_temp_anomaly": y_min},
        epochs=100,
        batch_size=32,
        validation_split=0.2,
        callbacks=callbacks_1,
        verbose=1
    )

    # =========================================================================
    # PHASE 2: CONTEXT FREEZE (LIVE FOCUS)
    # =========================================================================
    console.rule("[bold magenta]PHASE 2: Live Signal Focus[/]")
    
    for layer in weather_model.model.layers:
        if "ctx_lstm" in layer.name:
            console.print(f"  [cyan]-> Freezing layer:[/] {layer.name}")
            layer.trainable = False
            
    weather_model.model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=0.00001),
        loss="categorical_crossentropy",
        metrics=["accuracy", tf.keras.metrics.TopKCategoricalAccuracy(k=3, name="top_3_acc")]
    )

    patience_2 = 10
    callbacks_2 = [
        CSVLogger(csv_path, append=True),
        ModelCheckpoint(checkpoint_path, save_best_only=True, monitor="val_loss"),
        EarlyStopping(monitor="val_loss", patience=patience_2, restore_best_weights=True),
        EarlyStoppingProgress(patience=patience_2, prefix="Phase 2")
    ]

    weather_model.model.fit(
        {"main_input": X_live, "context_input": X_context}, 
        {"max_temp_anomaly": y_max, "min_temp_anomaly": y_min},
        epochs=50,
        batch_size=32,
        validation_split=0.2,
        callbacks=callbacks_2,
        verbose=1
    )

    console.print(Panel(f"[bold green]Staged Training complete![/]\nModel saved to: {checkpoint_path}", expand=False))

if __name__ == "__main__":
    train_area("sfbay")
