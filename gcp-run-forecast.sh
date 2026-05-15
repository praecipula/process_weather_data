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
  
  # Release the apt lock and wait for availability
  echo '[INFO] Preparing package manager (this may take a minute)...' && \
  sudo systemctl stop unattended-upgrades >/dev/null 2>&1 || true && \
  until sudo apt-get update >/dev/null 2>&1; do echo '  ...waiting for apt lock...'; sleep 10; done && \
  
  # Install necessary tools (cleanup old docker first)
  sudo apt-get remove -y containerd docker.io runc docker-ce docker-ce-cli containerd.io >/dev/null 2>&1 || true && \
  sudo apt-get install -y git gcsfuse docker-ce docker-ce-cli containerd.io docker-compose-plugin && \

  # Ensure docker-compose command is available
  if ! command -v docker-compose &> /dev/null; then sudo ln -sf /usr/libexec/docker/cli-plugins/docker-compose /usr/local/bin/docker-compose; fi && \

  # Add current user to docker group
  sudo usermod -aG docker \$(whoami) || true && \
  
  # Mount GCS
  echo '[INFO] Mounting GCS bucket...' && \
  sudo mkdir -p /mnt/gcs_mount_point && \
  sudo gcsfuse --implicit-dirs --uid=\$(id -u) --gid=\$(id -g) \"$BUCKET_NAME\" /mnt/gcs_mount_point || true && \
  
  # Sync Repo
  echo '[INFO] Cloning/Updating repository...' && \
  if [ -d /app/.git ]; then
    cd /app && sudo git fetch --all && sudo git reset --hard origin/\"$GIT_BRANCH\"
  else
    sudo rm -rf /app && sudo mkdir -p /app && sudo chown \$(whoami) /app && \
    git clone \"$REPO_URL\" /app && cd /app && git checkout \"$GIT_BRANCH\"
  fi && \
  
  # Download and patch reference notebook
  echo '[INFO] Fetching and patching reference notebook...' && \
  curl -s -o gencast_reference.ipynb https://raw.githubusercontent.com/google-deepmind/graphcast/main/gencast_demo_cloud_vm.ipynb && \
  ln -sf \"/mnt/gcs_mount_point/models/GenCast 0p25deg Operational <2022.npz\" \"GenCast 0p25deg Operational <2022.npz\" && \
  ln -sf \"/mnt/gcs_mount_point/era5_input/source-era5_date-${TARGET_DATE}_res-0.25_levels-13.nc\" \"source-era5_date-${TARGET_DATE}_res-0.25_levels-13.nc\" && \
  python3 patch_notebook.py gencast_reference.ipynb \"${TARGET_DATE}\" && \
  
  # Build and Run
  echo '[INFO] Building and running GenCast...' && \
  sudo docker-compose build --quiet && \
  sudo docker-compose run --rm \
    -e JAX_PLATFORM_NAME=tpu \
    -e JAX_PLATFORM_MODE=tpu_driver \
    gencast-worker \
    papermill gencast_reference.ipynb gencast_execution_log.ipynb && \
  
  echo '--- VM Setup End ---' && \
  sudo poweroff" || log_error "SSH command execution or GenCast run failed."

log_info "GenCast Prediction Workflow finished."
log_info "Check GCS bucket gs://$BUCKET_NAME for results."
