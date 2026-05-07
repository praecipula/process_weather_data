"""
keras_model.py
==============
Keras/TensorFlow implementation of the Weather prediction model.

Architecture:
  - Input: (MAX_SEQ_LEN, N_FEATURES)
  - Embedding: Station index (feature 27) maps to a dense vector.
  - Deep LSTM: Dual-layer LSTM for capturing complex temporal dependencies.
  - Dual Softmax Heads: Predicts probability distribution for max and min 
    temperature anomalies.
"""

import tensorflow as tf
from tensorflow.keras import layers, Model
from lib.constants import MAX_SEQ_LEN, N_FEATURES


class WeatherLSTM:
    """
    Keras model for predicting temperature anomalies using an LSTM backbone.
    """

    def __init__(self, area_config):
        self.area = area_config
        self.model = self._build_model()

    def _build_model(self) -> Model:
        # 1. Inputs
        main_input = layers.Input(shape=(MAX_SEQ_LEN, N_FEATURES), name="main_input")

        # 2. Branching: Separate Station Index from Continuous Features
        def slice_cont(x):
            return tf.concat([x[:, :, :27], x[:, :, 28:]], axis=-1)
        
        cont_features = layers.Lambda(slice_cont)(main_input)
        station_idx = layers.Lambda(lambda x: x[:, :, 27])(main_input)

        # 3. Embedding Layer for Station
        station_emb = layers.Embedding(
            input_dim=self.area.num_stations,
            output_dim=8,
            name="station_embedding"
        )(station_idx)

        # 4. Merge
        merged = layers.Concatenate(axis=-1)([cont_features, station_emb])

        # 5. LSTM Backbone (Upgraded)
        # Increased to 64 units and added a second layer
        x = layers.LSTM(64, return_sequences=True)(merged)
        x = layers.Dropout(0.2)(x)
        x = layers.LSTM(32, return_sequences=False)(x)
        x = layers.Dropout(0.2)(x)

        # 6. Dense Neck
        x = layers.Dense(64, activation="relu")(x)

        # 7. Dual Heads (Softmax)
        output_max = layers.Dense(
            self.area.temp_buckets, 
            activation="softmax", 
            name="max_temp_anomaly"
        )(x)
        
        output_min = layers.Dense(
            self.area.temp_buckets, 
            activation="softmax", 
            name="min_temp_anomaly"
        )(x)

        model = Model(inputs=main_input, outputs=[output_max, output_min])
        
        # 8. Compile with informative metrics
        # Top-3/Top-5 accuracy are great for weather (is the truth close?)
        metrics = [
            "accuracy",
            tf.keras.metrics.TopKCategoricalAccuracy(k=3, name="top_3_acc"),
            tf.keras.metrics.TopKCategoricalAccuracy(k=5, name="top_5_acc")
        ]

        model.compile(
            optimizer="adam",
            loss={
                "max_temp_anomaly": "categorical_crossentropy",
                "min_temp_anomaly": "categorical_crossentropy"
            },
            metrics={
                "max_temp_anomaly": metrics,
                "min_temp_anomaly": metrics
            }
        )
        
        return model

    def summary(self):
        self.model.summary()

    def train(self, X, y_max, y_min, epochs=50, batch_size=32, validation_split=0.2, callbacks=None):
        return self.model.fit(
            X, 
            {"max_temp_anomaly": y_max, "min_temp_anomaly": y_min},
            epochs=epochs,
            batch_size=batch_size,
            validation_split=validation_split,
            callbacks=callbacks
        )

    def predict_probs(self, X):
        return self.model.predict(X)
