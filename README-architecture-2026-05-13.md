# Architecture Log: 2026-05-13
## Regime: High-Fidelity Hybrid (Bayesian Observer)

### Status: PIVOTING
**Diagnosis**: The model reached a "Complexity Wall." Despite physically accurate PID signals and specialized heads, the feature-to-sample ratio (~83 features for ~7,000 samples) led to "Prior Dominance." The model preferred the safe daily average (the 7-day trend) over noisy 5-minute evidence, resulting in stagnant convergence plots.

---

### 1. Current Model Structure (The "State of the Art" as of May 13)
The model as of this morning uses a **Sensor Fusion** approach, combining four distinct temporal views:

*   View A: The Temporal vibe (Global Average Pooling): Summarizes the whole day's trend to capture "General Heat."
*   View B: The Day's Peak (Global Max Pooling): Explicitly identifies the highest temp seen so far to help collapse uncertainty after the peak.
*   View C: The Instantaneous "NOW" (Direct Feature Snapshot): Bypasses the LSTM to feed current Fog, Wind, and Circular Time directly into the decision layer.
*   View D: The Weekly Prior (Context LSTM): Provides the 7-day historical trend.

**Feature Vector (83 Dimensions)**:
- [0-30]: Raw physicals (Temp, Dewpoint, Pressure) and Geo metadata.
- [31-53]: Weather Multi-hot tokens (Fog, Mist, Rain, etc.).
- [54-77]: Cloud Grammar (Coverage and Altitude layers).
- [78-79]: Solar constraints (Daylight minutes, record proximity).
- [80-82]: PID Calculus (Proportional Anomaly, Integral Momentum, Derivative Velocity).

---

### 2. Intellectual Decision Points
*   Integral Heat Momentum: Successfully introduced to help the model distinguish between a random spike and a sustained heatwave.
*   Anti-Leak Resampling: Crucial fix implemented to prevent the model from "cheating" by seeing future interpolated slopes during training.
*   Logit Summation vs. Concatenation: Experimented with additive residual heads (interpretability) but returned to deep non-linear concatenation (performance).

---

### 3. The Pivot: "Radical Simplification"
Starting this afternoon, we are moving away from "The Big Brain" and toward **"The Sharp Signal."**

**The Plan**:
1.  **Feature Pruning**: Removing 70% of inputs. We will keep only the High-Signal Trinity:
    *   Proportional Anomaly (Current state)
    *   Integral Momentum (Accumulated state)
    *   The Context Prior (Historical state)
2.  **Label Smoothing**: Changing the loss function to give partial credit for "near misses" in temperature buckets.
3.  **Model Shrinkage**: Reducing hidden units to prevent memorization of noise.

### Why this is the "Professor's Path":
On a dataset of this size, a model that sees **too much** is a model that understands **nothing**. By blinding the model to the "noise" of cloud layers and individual wind directions, we force it to become an expert in the only thing that matters for convergence: **The Heat Gradient.**
