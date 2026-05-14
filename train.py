"""
train.py
========
Staged Training Protocol for 3-Way Multi-Resolution Architecture.
Phase 1: Full Discovery (Macro + Context).
Phase 2: Macro & Context Freeze (Micro-Reflex Focus).
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
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn

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
    console.print(f"[bold blue]--- Starting STAGED MULTI-RESOLUTION Training for Area: {area_key} ---[/]")
    area = get_area(area_key)
    builder = SequenceBuilder(area, session)
    
    console.print("Gathering data (This may take a while with 10 years of history)...")
    try:
        total_days = builder.get_total_days()
        
        X_macro, X_micro, X_context, y_max, y_min, metas = [], [], [], [], [], []
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            task = progress.add_task("[cyan]Processing days...", total=total_days)
            
            for day_samples in builder.iter_training_days():
                for sample in day_samples:
                    X_macro.append(sample[0])
                    X_micro.append(sample[1])
                    X_context.append(sample[2])
                    y_max.append(sample[3])
                    y_min.append(sample[4])
                    metas.append(sample[5])
                progress.advance(task)
                
        X_macro = np.stack(X_macro)
        X_micro = np.stack(X_micro)
        X_context = np.stack(X_context)
        y_max = np.stack(y_max)
        y_min = np.stack(y_min)
        
    except ValueError as e:
        console.print(f"[bold red]Error:[/] {e}")
        return

    console.print(f"[green]Dataset prepared:[/] {X_macro.shape[0]} samples")
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
    console.rule("[bold green]PHASE 1: Full Discovery (Macro + Context)[/]")
    patience_1 = 15
    callbacks_1 = [
        CSVLogger(csv_path, append=False),
        ModelCheckpoint(checkpoint_path, save_best_only=True, monitor="val_loss"),
        EarlyStopping(monitor="val_loss", patience=patience_1, restore_best_weights=True),
        EarlyStoppingProgress(patience=patience_1, prefix="Phase 1")
    ]

    history_1 = weather_model.model.fit(
        {
            "macro_input": X_macro, 
            "micro_input": X_micro, 
            "context_input": X_context
        }, 
        {"max_temp_anomaly": y_max, "min_temp_anomaly": y_min},
        epochs=300,
        batch_size=32,
        validation_split=0.2,
        callbacks=callbacks_1,
        verbose=1
    )

    # =========================================================================
    # PHASE 2: MACRO & CONTEXT FREEZE (MICRO FOCUS)
    # =========================================================================
    console.rule("[bold magenta]PHASE 2: Micro-Reflex Signal Focus[/]")
    
    # Freeze the Context and Macro branches
    for layer in weather_model.model.layers:
        if "ctx" in layer.name or "macro" in layer.name:
            console.print(f"  [cyan]-> Freezing layer:[/] {layer.name}")
            layer.trainable = False
            
    # Re-compile with an extremely low learning rate for fine-tuning the 5-min reflexes
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

    history_2 = weather_model.model.fit(
        {
            "macro_input": X_macro, 
            "micro_input": X_micro, 
            "context_input": X_context
        }, 
        {"max_temp_anomaly": y_max, "min_temp_anomaly": y_min},
        epochs=50,
        batch_size=32,
        validation_split=0.2,
        callbacks=callbacks_2,
        verbose=1
    )

    # Gather metrics for summary
    epochs_1 = len(history_1.history['loss'])
    best_loss_1 = min(history_1.history['val_loss'])
    epochs_2 = len(history_2.history['loss'])
    best_loss_2 = min(history_2.history['val_loss'])

    import yaml
    from zoneinfo import ZoneInfo
    summary_path = "summary.yaml"
    summary_data = {}
    if os.path.exists(summary_path):
        try:
            with open(summary_path, 'r') as f:
                summary_data = yaml.safe_load(f) or {}
        except: pass
    
    local_tz = ZoneInfo("America/Los_Angeles")
    now_iso = datetime.datetime.now(local_tz).isoformat()
    
    summary_data["last_training"] = {
        "timestamp": now_iso,
        "epochs_phase_1": epochs_1,
        "best_loss_phase_1": float(best_loss_1),
        "epochs_phase_2": epochs_2,
        "best_loss_phase_2": float(best_loss_2),
        "overall_best_loss": float(min(best_loss_1, best_loss_2))
    }
    
    with open(summary_path, 'w') as f:
        yaml.dump(summary_data, f, default_flow_style=False, sort_keys=False)

    console.print(Panel(f"[bold green]Staged Training complete![/]\nModel saved to: {checkpoint_path}\nSummary saved to: {summary_path}", expand=False))

if __name__ == "__main__":
    train_area("sfbay")
