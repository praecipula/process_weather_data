# Timezone and Data Integrity Investigation (2026-05-06)

## Problem Statement
We observed significant mismatches between the `summary_fcst` table (daily high/low labels) and the `weather` table (raw observations). In some cases, discrepancies were as high as 9°F, suggesting either incorrect ingestion, timezone misalignment, or dirty source data.

## Key Findings

### 1. Ingestion Gaps (The "Month-End" Bug)
We discovered that `ingest_historical.py` was missing the last day of every month. This was caused by the IEM API's use of **exclusive** end dates. By requesting data up to the 31st, the API excluded all observations *on* the 31st. 
*   **Resolution:** Modified ingestion logic to use the 1st of the following month as an exclusive boundary.

### 2. Timezone Misalignment (UTC vs. Local)
Raw observations are ingested in **UTC**, while labels represent a **Local Calendar Day** (America/Los_Angeles). A naive date comparison using UTC timestamps caused mismatches near the midnight boundaries.
*   **Resolution:** Implemented `zoneinfo` in validation and training logic to precisely map local day boundaries to their corresponding UTC windows, accounting for both PST (-8) and PDT (-7).

### 3. Source Data Noise (The "New Year's Ghost")
Even with perfect alignment, some days (notably 2024-01-01) showed high deltas (Label: 68°F vs. Obs: 59°F). External verification confirmed the 68°F label was likely a source-side error.
*   **Resolution:** Implemented defensive filtering at runtime to exclude days with excessive label-observation deltas (>5°F).

### 4. DST Transition Noise
Days where the clock shifts (Spring Forward/Fall Back) result in 23-hour or 25-hour local days. These are edge cases that introduce non-standard signal noise.
*   **Resolution:** We skip DST transition days during training to ensure the model learns from consistent 24-hour cycles.

## Core Philosophies & Future Patterns

### The "Unbroken Circle" vs. Local Discontinuities
Our model treats time as a **continuous, circular basis**. To the neural network, the atmosphere doesn't care about clocks; it cares about solar cycles and physical continuity.
*   **UTC as the Canonical Coordinate:** We only ever work with UTC in the database because it provides an unbroken timeline. This is the "Coordinate System" for our input vectors.
*   **Eliding Non-Conforming Data:** We intentionally elide (skip) input data that doesn't fit cleanly on this unbroken circle—specifically DST transition days. Attempting to force a 23-hour day into a 24-hour input vector creates "jagged" features that confuse the model.
*   **Boundary Disconnects:** While our inputs are continuous, our **forecast targets (labels)** are based on Local Time and *do* have discontinuities. By using UTC as the canonical base and only localizing at the "last mile" (to find the max/min within a local boundary), we keep the model's physical understanding of time separate from the arbitrary human conventions of the forecast target.

### Defensive Data Prep
We do not assume the source labels are perfect. The training pipeline must verify labels against observations and reject "impossible" or highly anomalous days. If the source data is dirty, we programmatically step over it rather than letting it poison the weights.
