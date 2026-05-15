# GCP Environment Setup for GenCast Forecasting

This document outlines the one-time setup required to prepare your Google Cloud Platform (GCP) project for running GenCast weather forecasts. The goal is to create a cost-effective, reproducible, and ephemeral compute environment.

## Architecture Overview

*   **Source Code:** Lives in your GitHub repository (this repo).
*   **Persistent Data:** Google Cloud Storage (GCS) buckets for model weights, input ERA5 data, and output forecast files (`.nc`).
*   **Compute:** Ephemeral Compute Engine VM + Cloud TPU, spun up only when a forecast run is needed.
*   **Environment:** Defined by `Dockerfile` and `docker-compose.yml` for consistency.
*   **Local Analysis:** GCS bucket mounted locally (via `gcsfuse`) for convenient analysis of `.nc` output files.

---

## 1. Google Cloud Project Setup

1.  **Create a New GCP Project:**
    *   Go to the [GCP Console](https://console.cloud.google.com/).
    *   Click the project selector dropdown (usually at the top).
    *   Click "New Project."
    *   Give it a meaningful name (e.g., `overengineeredweather`). Note down its Project ID (it's usually a generated string like `overengineeredweather-xxxxxx`).

2.  **Set Up Billing:**
    *   Ensure billing is enabled for your new project. Go to [Billing](https://console.cloud.google.com/billing) in the console and link a billing account. **TPUs are not covered by the free tier.**

---

## 2. Enable Required GCP APIs

From the [API Library](https://console.cloud.google.com/apis/library), enable the following APIs for your new project:

*   **Compute Engine API** (required for VMs and TPUs)
*   **Cloud TPU API** (required for Cloud TPUs)
*   **Cloud Storage API** (required for GCS buckets)
*   **Service Usage API** (often required for various GCP operations)

---

## 3. Create a Service Account & Grant Permissions

You'll need a service account for your ephemeral VM to securely access GCS and TPUs.

1.  **Create Service Account:**
    *   Go to [IAM & Admin -> Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts).
    *   Click "CREATE SERVICE ACCOUNT."
    *   Give it a name (e.g., `runnerserviceacct`).
    *   Click "DONE." (You don't need to grant roles here yet).

2.  **Grant Permissions to the Service Account:**
    *   Go to [IAM & Admin -> IAM](https://console.cloud.google.com/iam-admin/iam).
    *   Click "GRANT ACCESS."
    *   In the "New principals" field, enter the email of your newly created service account.
    *   Grant the following roles:
        *   `Storage Admin` (to read/write to your GCS buckets)
        *   `Compute Instance Admin (v1)` (to manage the VM)
        *   `Cloud TPU Admin` (to manage the TPU)
        *   `Service Account User` (to allow the VM to run as this service account)
    *   Click "SAVE."

---

## 4. Create Cloud Storage Buckets

You'll need at least one GCS bucket to store GenCast model weights, any custom input data, and all your forecast outputs.

1.  **Create a GCS Bucket:**
    *   Go to [Cloud Storage -> Buckets](https://console.cloud.google.com/storage/buckets).
    *   Click "CREATE BUCKET."
    *   Choose a globally unique name (e.g., `overengineeredweather-run-data`).
    *   Choose a region close to `us-central1` (e.g., `us-central1`) to minimize network latency with your TPUs.
    *   Choose "Standard" storage class.
    *   Retain default data protection options.
    *   Click "CREATE."

2.  **Note:** Make sure the service account created in Step 3 has `Storage Admin` role on this bucket.

---

## 5. Cloud TPU Quota Status (Verified)

**Good News:** Your project has a sufficient default quota for Preemptible TPU v5e instances in regions like `us-central1` and `us-east1`.

Our target instance is a `v5litepod-8` (8 cores), and your project's default limit for `Preemptible TPU v5 lite pod cores` is **16 cores**.

**No quota increase request is necessary.** You can proceed directly to the next steps.

---

## 6. Access GenCast Models and ERA5 Data

Both the GenCast model weights and the ERA5 input data are publicly available in GCS buckets provided by Google. You will copy the necessary model weights to your private GCS bucket.

1.  **GenCast Models and Stats:**
    *   The pre-trained GenCast models are in `gs://dm_graphcast/gencast/`. You need both the model weights (.npz) and the normalization statistics (.nc).
    *   **Copy Model Weights:**
        `gcloud storage cp "gs://dm_graphcast/gencast/params/GenCast 0p25deg Operational <2022.npz" gs://overengineeredweather-run-data/models/`
    *   **Copy Normalization Stats:**
        `gcloud storage cp -r gs://dm_graphcast/gencast/stats gs://overengineeredweather-run-data/`

2.  **ERA5 Data:**
    *   The ERA5 dataset is massive and lives in `gs://gcp-public-data-arco-era5/`. You will typically stream or copy only the specific atmospheric variables and time slices needed for your forecast.
    *   **Important:** Do NOT try to copy the entire ERA5 dataset. It's petabytes in size. Your scripts will need to intelligently select and download only the required input files for a given forecast run.

---

## 7. Local GCS FUSE Setup (for Analysis)

To seamlessly analyze `.nc` output files locally, you'll use `gcsfuse` to mount your GCS bucket.

1.  **Install `gcsfuse`:** Follow the installation instructions for your Linux distribution: [gcsfuse GitHub](https://github.com/GoogleCloudPlatform/gcsfuse/blob/master/docs/install.md).
2.  **Authenticate:**
    *   Log into the gcloud CLI: `gcloud auth login`
    *   **Crucial for gcsfuse:** Set up Application Default Credentials (ADC):
        `gcloud auth application-default login`
    *   Ensure your project is set: `gcloud config set project overengineeredweather`
3.  **Mount the Bucket:**
    *   Create a local mount point: `mkdir ~/gcs_mount_point`
    *   Mount your bucket: `gcsfuse --implicit-dirs overengineeredweather-run-data ~/gcs_mount_point`
    *   You can then access files in `gs://overengineeredweather-run-data/` via `~/gcs_mount_point/`.

---

## 8. Next Steps: Automating the GenCast Run

The following files will be created in your GitHub repository to automate the prediction workflow:

### `docker-compose.yml`

This file defines the Docker container environment where GenCast will run. It ensures all dependencies are encapsulated and the environment is consistent. It will also define how your local code and the GCS mount point are exposed to the container.

### `Dockerfile`

This file describes how to build the Docker image for the `gencast-worker` service defined in `docker-compose.yml`. It will install the necessary Python packages (JAX, Haiku, NetCDF libraries, etc.) and the GenCast codebase.

### `gcp-run-forecast.sh`

This is the main orchestration script. It will handle:
*   Provisioning a Compute Engine VM with an attached Cloud TPU.
*   Installing Docker and Docker Compose on the VM.
*   Cloning your GitHub repository onto the VM.
*   Mounting your GCS bucket onto the VM using `gcsfuse`.
*   Building and running the Docker container via `docker-compose`.
*   Executing the GenCast prediction script inside the container.
*   (Optionally) Copying any final results or logs to GCS if not already handled by the `gcsfuse` mount.
*   Spinning down and deleting the VM and TPU to save costs.

This script encapsulates the entire ephemeral workflow.

---

## 9. Data Ingestion Strategy

To run a forecast, GenCast requires a global snapshot of the atmosphere at two time steps: $T$ and $T-6$ hours.

### ERA5: The "Holdout" Test Set
*   **Training Window:** DeepMind trained GenCast on data from **1979 to 2018**.
*   **Holdout Group:** Any data from **2019 to Present** is technically a "holdout" and is perfect for testing the model's accuracy on unseen weather.
*   **Availability:**
    *   **ERA5 (Final):** 2-3 month delay.
    *   **ERA5T (Preliminary):** **5-day delay**.
*   **Usage:** Perfect for backtesting and "walking" the model through past events.

### HRES: Real-Time Operational Data
*   **Purpose:** For actual Kalshi arbitrage/betting, we cannot wait 5 days for ERA5T.
*   **Source:** ECMWF HRES (High Resolution Forecast) initialization data.
*   **Usage:** This provides the 0-hour latency needed for production runs.

### One-Time Setup: Copernicus CDS API
You must register for an account to download ERA5 data automatically via Python.

1.  **Register:** Create an account at [Copernicus Climate Data Store (CDS)](https://cds.climate.copernicus.eu/).
2.  **API Key:** Go to your profile page and find your **UID** and **API Key**.
3.  **Local Credentials:** Create a file named `.cdsapirc` in your home directory (`~/.cdsapirc`) with the following content:
    ```text
    url: https://cds.climate.copernicus.eu/api/v2
    key: YOUR_UID:YOUR_API_KEY
    ```
4.  **Accept Terms:** Ensure you have accepted the "Terms of Use" for the ERA5 dataset on the CDS website.

### Future Ingestion Script
We will build an `ingest_era5.py` script that uses the `cdsapi` library to:
1.  Request the 0.25° grid for the required 13 pressure levels.
2.  Package the $T$ and $T-6$ snapshots into a single `input_batch.nc` file.
3.  Upload the file to `gs://overengineeredweather-run-data/era5_input/`.
4.  Trigger the `gcp-run-forecast.sh` script.

---

## 10. Upstream Patches and Codemods (`patch_notebook.py`)

Because the DeepMind repository is primarily a research library, it often contains hardcoded paths, interactive widgets, or strict validation logic that is incompatible with an automated, headless pipeline.

### The Invariant
**All surgical modifications to the upstream source code must live in `patch_notebook.py`.**

This script acts as our "inline-diff factory." Instead of maintaining a fork of the DeepMind repository, we fetch the `main` branch dynamically and apply our changes on the fly. 

**Use this script for:**
*   **Path Injection**: Replacing empty strings (`MODEL_PATH = ""`) with our mounted GCS paths.
*   **Parameter Overrides**: Hardcoding ensemble counts or seed values.
*   **Bypassing UI logic**: Commenting out `ipywidgets` or interactive `print` statements that fail in headless environments.
*   **API Adaptation**: Renaming variables or dimensions if the upstream API changes.

By centralizing these "codemods" in one Python script, we maintain a clear audit trail of how we've adapted the research code for production use.