"""
keras_model.py
==============
Keras/TensorFlow implementation of the Weather prediction model.

Architecture:
  - Input: (MAX_SEQ_LEN, N_FEATURES)
  - Embedding: Station index (feature 27) maps to a dense vector.
  - LSTM: Processes the temporal sequence of 288 observations.
  - Dual Softmax Heads: Predicts probability distribution for max and min 
    temperature anomalies (-20F to +20F).
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
        # Full feature vector: (MAX_SEQ_LEN, 80)
        main_input = layers.Input(shape=(MAX_SEQ_LEN, N_FEATURES), name="main_input")

        # 2. Branching: Separate Categorical from Continuous
        # Station index is at index 27.
        # We'll use Lambda layers to slice the input tensor.
        
        # Continuous features (everything except index 27)
        # Note: In a production setting, we might want to be more surgical here, 
        # but for a prototype, we'll slice around the station index.
        def slice_cont(x):
            return tf.concat([x[:, :, :27], x[:, :, 28:]], axis=-1)
        
        cont_features = layers.Lambda(slice_cont)(main_input)
        
        # Station Index for Embedding
        station_idx = layers.Lambda(lambda x: x[:, :, 27])(main_input)

        # 3. Embedding Layer for Station
        # num_stations is usually small (4-10); embedding size 4-8 is plenty.
        station_emb = layers.Embedding(
            input_dim=self.area.num_stations,
            output_dim=8,
            name="station_embedding"
        )(station_idx)

        # 4. Merge
        # Concatenate the dense station embedding with the continuous features
        merged = layers.Concatenate(axis=-1)([cont_features, station_emb])

        # 5. LSTM Backbone
        # Significantly downsized for small (20-day) dataset
        # A single 8-unit layer is harder to overfit.
        x = layers.LSTM(8, return_sequences=False)(merged)
        x = layers.Dropout(0.1)(x)

        # 6. Dense Neck (Simplified)
        # No extra hidden layers, go straight to specialized heads.

        # 7. Dual Heads (Softmax)
        # Output is the probability distribution across the anomaly buckets.
        # e.g. 41 units for -20 to +20.
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
        
        model.compile(
            optimizer="adam",
            loss={
                "max_temp_anomaly": "categorical_crossentropy",
                "min_temp_anomaly": "categorical_crossentropy"
            },
            metrics={
                "max_temp_anomaly": "accuracy",
                "min_temp_anomaly": "accuracy"
            }
        )
        
        return model

    def summary(self):
        self.model.summary()

    def train(self, X, y_max, y_min, epochs=50, batch_size=32, validation_split=0.2, callbacks=None):
        """
        Train the model on prepared numpy arrays.
        X: (samples, MAX_SEQ_LEN, N_FEATURES)
        y_max: (samples, temp_buckets)
        y_min: (samples, temp_buckets)
        """
        return self.model.fit(
            X, 
            {"max_temp_anomaly": y_max, "min_temp_anomaly": y_min},
            epochs=epochs,
            batch_size=batch_size,
            validation_split=validation_split,
            callbacks=callbacks
        )

    def predict_probs(self, X):
        """
        Returns (max_probs, min_probs)
        Each is (samples, temp_buckets)
        """
        return self.model.predict(X)
