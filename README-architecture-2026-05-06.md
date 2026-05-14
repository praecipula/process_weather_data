# Weather Model Architecture: Hybrid Dual-Head LSTM (2026-05-06)

## Overview
This document outlines the architectural strategy for the weather prediction model. Our goal is to predict daily high and low temperature anomalies by reconciling short-term convective physics with long-term atmospheric inertia.

## The Dual-Head Strategy: Two Temporal Lenses
The model uses a **Multi-Scale Temporal Encoding** approach, processing data through two distinct "heads" before fusion.

### 1. The "Live" Head (Micro-scale)
*   **Input:** High-resolution 5-minute METAR observations for the current calendar day.
*   **Dimensions:** 288 steps x 80 features.
*   **Architecture:** Dual-layer LSTM (64/32 units).
*   **Purpose:** Captures immediate convective developments—cloud cover changes, sea breeze onset, and the real-time heating/cooling curve. It learns the "Potential" of the day.

### 2. The "Context" Head (Macro-scale)
*   **Input:** Daily summary data for the preceding 7 days.
*   **Dimensions:** 7 steps x 10 features.
*   **Architecture:** Single-layer LSTM (32 units).
*   **Purpose:** Captures "Atmospheric Inertia." If a region has been in a high-pressure ridge for a week, today is physically more likely to over-perform the climatological normal. This head provides the "Setup" for the day.

## Core Design Decisions

### "Unbroken Circle" vs. "Local Discontinuity"
*   **Unbroken Circle (Inputs):** The model's inputs are fundamentally continuous physical processes. We treat **UTC as our canonical timeline** to ensure no temporal "jumps" or overlaps. We explicitly **elide (skip) DST transition days** during training because a 23-hour day creates a "jagged" circle that confuses the LSTM's understanding of rates of change.
*   **Local Discontinuities (Targets):** While weather is physical, human forecasts (labels) are arbitrary calendar boundaries. We localize our data at the "last mile"—mapping the continuous UTC stream onto local America/Los_Angeles day boundaries only when defining the target high/low.

### Physical Whitelisting over PCA
We intentionally chose a **physically grounded 10-feature whitelist** for the Context head rather than using Principal Component Analysis (PCA) or blind feature explosion.
*   **Why not PCA?** PCA is linear and may collapse physically distinct signals (like high pressure vs. high humidity) into a single "component" that loses the nuance required for a non-linear LSTM.
*   **The Approach:** We pick features representing the "Axes of Reality" (Pressure, Humidity, Momentum, Solar Budget) and let the neural network's internal weights and dropout layers perform the non-linear "pruning."

## Model Topology
1.  **Independent Branches:** Live and Context sequences are processed in parallel through their respective LSTMs.
2.  **Fusion:** The final states of both LSTMs are concatenated into a single dense vector.
3.  **Specialized Heads:** The merged vector passes through a shared dense "neck" (64 units) before splitting into two Softmax heads (Max Temp Anomaly and Min Temp Anomaly).

## Performance Metrics
Because accuracy in a 41-bucket classification task is "all or nothing," we evaluate success using:
*   **Exact Accuracy:** % of correct 1-degree predictions.
*   **Top-3/Top-5 Accuracy:** % of time the true temperature is among the model's most likely guesses. This measures the model's "Neighborhood" reliability.
