"""
keras_model.py
==============
Multi-Resolution Architecture.
1. Context Head: 7 days of daily summaries.
2. Macro Head: 24 hours of hourly data (Deep Physics).
3. Micro Head: 120 minutes of 5-minute data (High-Frequency Reflexes).
"""

import tensorflow as tf
from tensorflow.keras import layers, Model
from lib.constants import MACRO_SEQ_LEN, MICRO_SEQ_LEN, N_FEATURES

class WeatherLSTM:
    def __init__(self, area_config):
        self.area = area_config
        self.model = self._build_model()

    def _build_model(self) -> Model:
        # 1. Inputs
        macro_input = layers.Input(shape=(MACRO_SEQ_LEN, N_FEATURES), name="macro_input")
        micro_input = layers.Input(shape=(MICRO_SEQ_LEN, N_FEATURES), name="micro_input")
        context_input = layers.Input(shape=(7, 1), name="context_input")

        # 2. Macro Branch (Hourly Deep Physics)
        x_macro = layers.LSTM(64, return_sequences=True, name="macro_lstm")(macro_input)
        x_macro = layers.Dropout(0.1)(x_macro)
        macro_avg = layers.GlobalAveragePooling1D(name="macro_avg")(x_macro)
        macro_max = layers.GlobalMaxPooling1D(name="macro_max")(x_macro)

        # 3. Micro Branch (120-min High-Resolution Reflex)
        x_micro = layers.LSTM(32, return_sequences=True, name="micro_lstm")(micro_input)
        # We flatten the micro sequence because we want the exact trajectory/shape of the last 2 hours
        x_micro_flat = layers.Flatten(name="micro_flatten")(x_micro)
        x_micro_dense = layers.Dense(32, activation="relu", name="micro_dense")(x_micro_flat)

        # 4. Context Branch (7-day Trend)
        x_ctx = layers.LSTM(16, name="ctx_lstm")(context_input)

        # 5. The Sensor Fusion Neck
        merged = layers.Concatenate(axis=-1)([macro_avg, macro_max, x_micro_dense, x_ctx])
        
        x = layers.Dense(128, activation="relu", name="neck_1")(merged)
        x = layers.Dropout(0.1)(x)
        x = layers.Dense(64, activation="relu", name="neck_2")(x)

        # 6. Dual Heads
        output_max = layers.Dense(self.area.temp_buckets, activation="softmax", name="max_temp_anomaly")(x)
        output_min = layers.Dense(self.area.temp_buckets, activation="softmax", name="min_temp_anomaly")(x)

        model = Model(inputs=[macro_input, micro_input, context_input], outputs=[output_max, output_min])
        
        # 7. Compile (No Label Smoothing for strict gradient)
        optimizer = tf.keras.optimizers.Adam(learning_rate=0.0005)
        metrics = ["accuracy", tf.keras.metrics.TopKCategoricalAccuracy(k=3, name="top_3_acc")]
        model.compile(optimizer=optimizer, loss="categorical_crossentropy", metrics=metrics)
        
        return model

    def summary(self):
        self.model.summary()

    def predict_probs(self, X):
        return self.model.predict(X)
