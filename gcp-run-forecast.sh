#!/bin/bash

# gcp-run-forecast.sh
# Automates the provisioning, execution, and teardown of a GenCast prediction job on GCP.

# --- Configuration (REPLACE THESE PLACEHOLDERS WITH YOUR ACTUAL VALUES) ---
PROJECT_ID="overengineeredweather"             # Your GCP Project ID
ZONE="us-east5-a"                         # GCP Zone for VM and TPU
TPU_NAME="gencast-tpu"                       # Name for your TPU VM instance
TPU_TYPE="v5p-8"                             # Use v5p (Performance) which has 768 cores of quota
VM_MACHINE_TYPE="n2-standard-8"              # Machine type for the accompanying VM
BUCKET_NAME="overengineeredweather-run-data" # Your GCS Bucket Name for data and models
REPO_URL="https://github.com/praecipula/process_weather_data.git" # URL to your GitHub repository
GIT_BRANCH="main"                            # Git branch to checkout
SERVICE_ACCOUNT_EMAIL="runnerserviceacct@overengineeredweather.iam.gserviceaccount.com" # Service Account Email

# --- Command Line Arguments ---
TARGET_DATE="2024-05-10"
if [ "$1" == "--date" ] && [ -n "$2" ]; then
  TARGET_DATE="$2"
fi

# --- Helper Functions for Robustness ---
function log_info {
  echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') $1"
}

function log_error {
  echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') $1" >&2
  exit 1
}

# --- Main Script Logic ---

log_info "Starting GenCast Prediction Workflow."

# Ensure gcloud is configured for the correct project and zone
gcloud config set project "$PROJECT_ID" || log_error "Failed to set gcloud project to $PROJECT_ID."
gcloud config set compute/zone "$ZONE" || log_error "Failed to set gcloud compute zone to $ZONE."

# 1. Check if TPU VM exists, create if not
if gcloud compute tpus tpu-vm describe "$TPU_NAME" --zone="$ZONE" &>/dev/null; then
  log_info "TPU VM $TPU_NAME already exists in zone $ZONE. Skipping creation."
else
  log_info "Provisioning TPU VM ($TPU_NAME) with TPU type $TPU_TYPE in zone $ZONE..."
  gcloud compute tpus tpu-vm create "$TPU_NAME" \
    --zone="$ZONE" \
    --accelerator-type="$TPU_TYPE" \
    --version="v2-alpha-tpuv5" \
    --service-account="$SERVICE_ACCOUNT_EMAIL" \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --preemptible || log_error "Failed to provision TPU VM $TPU_NAME."
fi

log_info "TPU VM $TPU_NAME provisioned. Waiting for SSH to become available..."

# Wait a bit for the SSH service to come up on the VM
sleep 60 # Adjust this based on VM boot time if needed

# 2. SSH into VM and execute setup/run commands
log_info "SSHing into VM ($TPU_NAME) to perform setup and run GenCast."
gcloud compute tpus tpu-vm ssh "$TPU_NAME" --zone="$ZONE" --worker=all --command="
  echo '--- VM Setup Start ---' && \
  
  # Disable background updates to free up the package manager
  echo '[INFO] Disabling background updates...' && \
  sudo systemctl stop unattended-upgrades && \
  sudo systemctl disable unattended-upgrades && \
  
  # Wait for any in-progress background updates to finish
  echo '[INFO] Waiting for any remaining apt-get locks to be released...' && \
  while sudo fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1; do sleep 5; done && \
  
  # Update apt-get and install necessary tools
  echo '[INFO] Adding GCS FUSE repository...' && \
  export GCSFUSE_REPO=gcsfuse-\$(lsb_release -c -s) && \
  echo \"deb https://packages.cloud.google.com/apt \$GCSFUSE_REPO main\" | sudo tee /etc/apt/sources.list.d/gcsfuse.list && \
  curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add - && \
  echo '[INFO] Updating apt-get and installing Git, Docker, Docker Compose, gcsfuse...' && \
  sudo apt-get update && \
  sudo apt-get remove -y containerd docker.io runc docker-ce docker-ce-cli containerd.io && \
  sudo apt-get install -y git gcsfuse docker-ce docker-ce-cli containerd.io docker-compose-plugin && \

  # Ensure docker-compose command is available (alias to plugin if needed)
  if ! command -v docker-compose &> /dev/null; then sudo ln -s /usr/libexec/docker/cli-plugins/docker-compose /usr/local/bin/docker-compose; fi && \

  # Add the current user to the docker group
  sudo usermod -aG docker \$(whoami) && \
  
  # Mount the GCS bucket using gcsfuse
  echo '[INFO] Mounting GCS bucket gs://$BUCKET_NAME to /mnt/gcs_mount_point...' && \
  sudo mkdir -p /mnt/gcs_mount_point && \
  sudo gcsfuse --implicit-dirs --uid=\$(id -u) --gid=\$(id -g) \"$BUCKET_NAME\" /mnt/gcs_mount_point && \
  
  # Clone the GitHub repository
  echo '[INFO] Cloning repository $REPO_URL...' && \
  sudo rm -rf /app && \
  sudo mkdir -p /app && \
  sudo chown \$(whoami) /app && \
  git clone \"$REPO_URL\" /app && \
  cd /app && \
  git checkout \"$GIT_BRANCH\" && \
  
  # Download the official reference notebook
  echo '[INFO] Fetching official GenCast reference notebook...' && \
  curl -o gencast_reference.ipynb https://raw.githubusercontent.com/google-deepmind/graphcast/main/gencast_demo_cloud_vm.ipynb && \
  
  # Patch the notebook directly (bypassing Papermill tagging bugs)
  # We use symlinks to bypass the notebook's brittle filename parsing logic
  echo '[INFO] Creating symlinks to satisfy notebook filename parsing...' && \
  ln -s \"/mnt/gcs_mount_point/models/GenCast 0p25deg Operational <2022.npz\" \"GenCast 0p25deg Operational <2022.npz\" && \
  ln -s \"/mnt/gcs_mount_point/era5_input/source-era5_date-${TARGET_DATE}_res-0.25_levels-13.nc\" \"source-era5_date-${TARGET_DATE}_res-0.25_levels-13.nc\" && \
  
  echo '[INFO] Patching notebook logic and paths...' && \
  sed -i "s|MODEL_PATH = \\\"\\\"|MODEL_PATH = \\\"GenCast 0p25deg Operational <2022.npz\\\"|g" gencast_reference.ipynb && \
  sed -i "s|DATA_PATH = \\\"\\\"|DATA_PATH = \\\"source-era5_date-${TARGET_DATE}_res-0.25_levels-13.nc\\\"|g" gencast_reference.ipynb && \
  sed -i "s|STATS_DIR = \\\"\\\"|STATS_DIR = \\\"/mnt/gcs_mount_point/stats/\\\"|g" gencast_reference.ipynb && \
  sed -i "s|num_ensemble_members = 8|num_ensemble_members = 50|g" gencast_reference.ipynb && \
  
  # Build and run the Docker container using docker-compose
  echo '[INFO] Building Docker image...' && \
  sudo docker-compose build && \
  
  echo '[INFO] Running GenCast prediction via Papermill (Reference Logic)...' && \
  sudo docker-compose run --rm \
    -e JAX_PLATFORM_NAME=tpu \
    -e JAX_PLATFORM_MODE=tpu_driver \
    gencast-worker \
    papermill gencast_reference.ipynb gencast_execution_log.ipynb && \
  
  echo '--- VM Setup End ---' && \
  echo '[INFO] GenCast execution complete. Powering off VM.' && \
  sudo poweroff" || log_error "SSH command execution or GenCast run failed."

log_info "GenCast Prediction Workflow finished."
log_info "Check GCS bucket gs://$BUCKET_NAME for results."
