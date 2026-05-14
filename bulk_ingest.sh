#!/bin/bash

# bulk_ingest.sh
# Automates the historical data ingestion to expand our dataset backward.

STATION="KSFO"
NCEI_ID="USW00023234"

echo "--- Phase 1: Ingesting Hourly Normals ---"
pipenv run python ingest_normals.py --station $STATION --ncei_id $NCEI_ID

echo -e "\n--- Phase 2: Ingesting Observations & Labels ---"
# We already have 2020-2026. Ingesting the previous decade for the Macro Head.
for year in 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019; do
    echo "Processing Year: $year"
    
    # Ingest hourly observations
    pipenv run python ingest_historical.py --station $STATION --year $year
    
    # Ingest daily high/low labels
    pipenv run python ingest_labels.py --station $STATION --year $year
    
    echo "Year $year complete."
done

echo -e "\n--- Bulk Ingest Complete ---"
echo "You can now run 'train.py' to train on the new data."
