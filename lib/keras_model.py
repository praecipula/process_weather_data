"""
keras_model.py
==============
Dual-Pooling Architecture.
Combines Average Pooling (the 'vibe') and Max Pooling (the 'peak') 
to ensure the model acknowledges when the day's high has been reached.
"""

import tensorflow as tf
from tensorflow.keras import layers, Model
from lib.constants import MAX_SEQ_LEN, N_FEATURES


class WeatherLSTM:
    def __init__(self, area_config):
        self.area = area_config
        self.model = self._build_model()

    def _build_model(self) -> Model:
        live_input = layers.Input(shape=(MAX_SEQ_LEN, N_FEATURES), name="main_input")
        context_input = layers.Input(shape=(7, 10), name="context_input")

        # 1. Sequence Pre-processing
        def slice_cont(x): return tf.concat([x[:, :, :27], x[:, :, 28:]], axis=-1)
        live_cont = layers.Lambda(slice_cont)(live_input)
        station_idx = layers.Lambda(lambda x: x[:, :, 27])(live_input)
        station_emb = layers.Embedding(input_dim=self.area.num_stations, output_dim=16)(station_idx)
        live_merged = layers.Concatenate(axis=-1)([live_cont, station_emb])

        # 2. The Observer (LSTM)
        x_live = layers.LSTM(128, return_sequences=True, name="observer_lstm")(live_merged)
        x_live = layers.Dropout(0.1)(x_live)
        
        # 3. DUAL TEMPORAL POOLING
        # Average tells us the trend; Max tells us the peak so far.
        avg_pool = layers.GlobalAveragePooling1D(name="avg_pool")(x_live)
        max_pool = layers.GlobalMaxPooling1D(name="max_pool")(x_live)
        
        # Combine the 'Vibe' and the 'Peak'
        live_features = layers.Concatenate(name="pool_concat")([avg_pool, max_pool])

        # 4. Context Branch (The Prior)
        # We name this clearly so we can freeze it later
        x_context = layers.LSTM(32, name="ctx_lstm")(context_input)
        
        # 5. The Decision Neck
        merged = layers.Concatenate(axis=-1)([live_features, x_context])
        
        x = layers.Dense(128, activation="relu", name="neck_1")(merged)
        x = layers.Dropout(0.1)(x)
        x = layers.Dense(64, activation="relu", name="neck_2")(x)

        # 6. Dual Heads
        output_max = layers.Dense(self.area.temp_buckets, activation="softmax", name="max_temp_anomaly")(x)
        output_min = layers.Dense(self.area.temp_buckets, activation="softmax", name="min_temp_anomaly")(x)

        model = Model(inputs=[live_input, context_input], outputs=[output_max, output_min])
        
        optimizer = tf.keras.optimizers.Adam(learning_rate=0.0001)
        metrics = ["accuracy", tf.keras.metrics.TopKCategoricalAccuracy(k=3, name="top_3_acc")]
        model.compile(optimizer=optimizer, loss="categorical_crossentropy", metrics=metrics)
        
        return model

    def summary(self):
        self.model.summary()

    def predict_probs(self, X):
        return self.model.predict(X)
