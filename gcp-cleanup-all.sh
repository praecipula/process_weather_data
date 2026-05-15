#!/bin/bash

# gcp-cleanup-all.sh
# "Peace of Mind" script to definitively tear down ALL compute resources in the project.
# Preserves Cloud Storage, but deletes all VMs, TPUs, and Queued Resources.

PROJECT_ID="overengineeredweather"

function log_info {
  echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') $1"
}

log_info "Starting global cleanup for project: $PROJECT_ID"

# 1. Cleanup TPU Queued Resources (Modern v5e/v4 provisioning)
log_info "Checking for orphaned TPU Queued Resources..."
QR_LIST=$(gcloud compute tpus queued-resources list --project="$PROJECT_ID" --format="value(name,zone)" 2>/dev/null)
if [ -n "$QR_LIST" ]; then
    while read -r name zone; do
        log_info "Deleting Queued Resource: $name in zone $zone"
        gcloud compute tpus queued-resources delete "$name" --zone="$zone" --project="$PROJECT_ID" --quiet --async
    done <<< "$QR_LIST"
else
    log_info "No Queued Resources found."
fi

# 2. Cleanup TPU VMs
log_info "Checking for orphaned TPU VMs..."
TPU_LIST=$(gcloud compute tpus tpu-vm list --project="$PROJECT_ID" --format="value(name,zone)" 2>/dev/null)
if [ -n "$TPU_LIST" ]; then
    while read -r name zone; do
        log_info "Deleting TPU VM: $name in zone $zone"
        gcloud compute tpus tpu-vm delete "$name" --zone="$zone" --project="$PROJECT_ID" --quiet --async
    done <<< "$TPU_LIST"
else
    log_info "No TPU VMs found."
fi

# 3. Cleanup Standard Compute Instances
log_info "Checking for orphaned Compute Engine Instances..."
VM_LIST=$(gcloud compute instances list --project="$PROJECT_ID" --format="value(name,zone)" 2>/dev/null)
if [ -n "$VM_LIST" ]; then
    while read -r name zone; do
        log_info "Deleting Instance: $name in zone $zone"
        gcloud compute instances delete "$name" --zone="$zone" --project="$PROJECT_ID" --quiet --async
    done <<< "$VM_LIST"
else
    log_info "No standard VM instances found."
fi

log_info "Cleanup commands issued. Most deletions are running asynchronously in the background."
log_info "You can verify the final state in the GCP Console under Compute Engine and TPU."
