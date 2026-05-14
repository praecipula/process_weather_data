#!/bin/bash

# bulk_ingest.sh
# Automates the historical data ingestion for 2023 and 2024.

STATION="KSFO"
NCEI_ID="USW00023234"

echo "--- Phase 1: Ingesting Hourly Normals ---"
pipenv run python ingest_normals.py --station $STATION --ncei_id $NCEI_ID

echo -e "\n--- Phase 2: Ingesting Observations & Labels ---"
for year in 2020 2021 2022 2023 2024 2025 2026; do
    echo "Processing Year: $year"
    
    # Ingest hourly observations
    pipenv run python ingest_historical.py --station $STATION --year $year
    
    # Ingest daily high/low labels
    pipenv run python ingest_labels.py --station $STATION --year $year
    
    echo "Year $year complete."
done

echo -e "\n--- Bulk Ingest Complete ---"
echo "You can now run 'train.py' to train on the new data."
