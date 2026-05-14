#!/bin/bash

# gcp-run-forecast.sh
# Automates the provisioning, execution, and teardown of a GenCast prediction job on GCP.

# --- Configuration (REPLACE THESE PLACEHOLDERS WITH YOUR ACTUAL VALUES) ---
PROJECT_ID="overengineeredweather"             # Your GCP Project ID
ZONE="us-central1-a"                         # GCP Zone for VM and TPU (v5e is common here)
TPU_NAME="gencast-tpu"                       # Name for your TPU VM instance
TPU_TYPE="v5litepod-8"                       # Use v5e with 8 cores
VM_MACHINE_TYPE="n2-standard-8"              # Machine type for the accompanying VM
BUCKET_NAME="overengineeredweather-run-data" # Your GCS Bucket Name for data and models
REPO_URL="git@github.com:praecipula/process_weather_data.git" # URL to your GitHub repository
GIT_BRANCH="main"                            # Git branch to checkout
SERVICE_ACCOUNT_EMAIL="gencast-runner@overengineeredweather.iam.gserviceaccount.com" # Service Account Email

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

# 1. Create TPU VM (which also creates an associated Compute Engine VM)
log_info "Provisioning TPU VM ($TPU_NAME) with TPU type $TPU_TYPE and VM type $VM_MACHINE_TYPE in zone $ZONE..."
gcloud compute tpus tpu-vm create "$TPU_NAME" 
  --zone="$ZONE" 
  --accelerator-type="$TPU_TYPE" 
  --version="tpu-ubuntu-22.04" 
  --machine-type="$VM_MACHINE_TYPE" 
  --service-account="$SERVICE_ACCOUNT_EMAIL" 
  --scopes=https://www.googleapis.com/auth/cloud-platform 
  --preemptible || log_error "Failed to provision TPU VM $TPU_NAME."

log_info "TPU VM $TPU_NAME provisioned. Waiting for SSH to become available..."

# Wait a bit for the SSH service to come up on the VM
sleep 60 # Adjust this based on VM boot time if needed

# 2. SSH into VM and execute setup/run commands
log_info "SSHing into VM ($TPU_NAME) to perform setup and run GenCast."
gcloud compute tpus tpu-vm ssh "$TPU_NAME" --zone="$ZONE" --worker=all --command="
  echo '--- VM Setup Start ---' && 
  
  # Update apt-get and install necessary tools (git, docker, docker-compose, gcsfuse)
  log_info 'Updating apt-get and installing Git, Docker, Docker Compose, gcsfuse...' && 
  sudo apt-get update && 
  sudo apt-get install -y git docker.io docker-compose gcsfuse && 
  
  # Add the current user to the docker group to run docker commands without sudo
  sudo usermod -aG docker "$(whoami)" && 
  
  # Mount the GCS bucket using gcsfuse
  log_info 'Mounting GCS bucket gs://$BUCKET_NAME to /mnt/gcs_mount_point...' && 
  sudo mkdir -p /mnt/gcs_mount_point && 
  sudo gcsfuse --implicit-dirs --uid=$(id -u) --gid=$(id -g) \"$BUCKET_NAME\" /mnt/gcs_mount_point && 
  
  # Clone the GitHub repository
  log_info 'Cloning repository $REPO_URL...' && 
  # Ensure /app is clean before cloning
  sudo rm -rf /app && 
  git clone \"$REPO_URL\" /app && 
  cd /app && 
  git checkout \"$GIT_BRANCH\" && 
  
  # Build and run the Docker container using docker-compose
  log_info 'Building Docker image...' && 
  docker-compose build && 
  
  log_info 'Running GenCast prediction via Docker Compose...' && 
  # NOTE: Replace 'python graphcast/scripts/run_gencast.py' with the actual command
  # to execute the GenCast inference within the container.
  # This typically involves setting JAX_PLATFORM_NAME=tpu and other environment variables.
  docker-compose run --rm 
    -e JAX_PLATFORM_NAME=tpu 
    -e JAX_PLATFORM_MODE=tpu_driver 
    gencast-worker 
    python graphcast/scripts/run_gencast.py \
    --input_data_path=\"/mnt/gcs_mount_point/era5_input/\" \
    --output_data_path=\"/mnt/gcs_mount_point/gencast_output/\" \
    --model_path=\"/mnt/gcs_mount_point/models/GenCast 0p25deg Operational <2022.npz\" \
    --stats_path=\"/mnt/gcs_mount_point/stats/\" && \
  
  echo '--- VM Setup End ---' && 
  log_info 'GenCast execution complete. Powering off VM.' && 
  # Power off the VM. The 'tpu-vm create' command will auto-delete upon poweroff.
  sudo poweroff" || log_error "SSH command execution or GenCast run failed."

# 3. Delete VM and TPU (redundant if startup-script includes poweroff, but good for explicit cleanup)
#    This step is primarily for scenarios where the startup-script doesn't poweroff
#    or if the script fails before poweroff. Uncomment if you need explicit cleanup.
# log_info "Ensuring VM and TPU ($TPU_NAME) are deleted in zone $ZONE..."
# gcloud compute tpus tpu-vm delete "$TPU_NAME" --zone="$ZONE" -q --async # --async allows script to finish immediately
# gcloud compute instances delete "$VM_NAME" --zone="$ZONE" -q --async

log_info "GenCast Prediction Workflow finished."
log_info "Check GCS bucket gs://$BUCKET_NAME for results."
log_info "You can mount your GCS bucket locally using 'gcsfuse $BUCKET_NAME ~/gcs_mount_point' to access results."
