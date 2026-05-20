#!/bin/bash

# gcp-cleanup-all.sh
# "Peace of Mind" script to definitively tear down ALL compute resources in the project.
# Preserves Cloud Storage, but deletes all VMs, TPUs, and Queued Resources.

PROJECT_ID="overengineeredweather"

function log_info {
  echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') $1"
}

log_info "Starting global cleanup for project: $PROJECT_ID"

# Keep track of async operations
OPERATIONS=()

# 1. Cleanup TPU Queued Resources (Modern v5e/v4 provisioning)
log_info "Checking for orphaned TPU Queued Resources..."
QR_LIST=$(gcloud compute tpus queued-resources list --project="$PROJECT_ID" --format="value(name,zone)" 2>/dev/null)
if [ -n "$QR_LIST" ]; then
    while read -r name zone; do
        log_info "Deleting Queued Resource: $name in zone $zone"
        # Capture the operation ID
        OUT=$(gcloud compute tpus queued-resources delete "$name" --zone="$zone" --project="$PROJECT_ID" --quiet --async 2>&1)
        OP=$(echo "$OUT" | grep -oE "operations/[^]]+" | head -1)
        if [ -n "$OP" ]; then OPERATIONS+=("$OP"); fi
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
        OUT=$(gcloud compute tpus tpu-vm delete "$name" --zone="$zone" --project="$PROJECT_ID" --quiet --async 2>&1)
        OP=$(echo "$OUT" | grep -oE "operations/[^]]+" | head -1)
        if [ -n "$OP" ]; then OPERATIONS+=("$OP"); fi
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
        OUT=$(gcloud compute instances delete "$name" --zone="$zone" --project="$PROJECT_ID" --quiet --async 2>&1)
        OP=$(echo "$OUT" | grep -oE "operations/[^]]+" | head -1)
        if [ -n "$OP" ]; then OPERATIONS+=("$OP"); fi
    done <<< "$VM_LIST"
else
    log_info "No standard VM instances found."
fi

if [ ${#OPERATIONS[@]} -eq 0 ]; then
    log_info "No active deletions to track."
    exit 0
fi

log_info "Tracking ${#OPERATIONS[@]} async operations. Waiting for completion (max 5m)..."

# 4. Wait Loop (up to 5 minutes)
for i in {1..30}; do
    ALL_DONE=true
    for op in "${OPERATIONS[@]}"; do
        # Extract the operation ID from the full path if necessary
        # Most tpu-vm commands return 'locations/ZONE/operations/ID'
        # Compute instances use a different format, but we'll try to describe whatever we caught
        STATUS=$(gcloud compute tpus tpu-vm operations describe "$op" --project="$PROJECT_ID" --format="value(done)" 2>/dev/null)
        
        # If gcloud call failed (e.g. for standard VMs which have different API), 
        # we treat as 'done' for tracking purposes or skip.
        if [ "$STATUS" != "True" ]; then
            ALL_DONE=false
            break
        fi
    done

    if [ "$ALL_DONE" = true ]; then
        log_info "All cloud resources successfully torn down."
        exit 0
    fi
    
    echo -n "."
    sleep 10
done

log_info "Timeout reached. Some deletions may still be in progress."
log_info "Check the GCP Console for final status."

