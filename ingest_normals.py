"""
ingest_normals.py
=================
Downloads 1991-2020 hourly climate normals from NCEI for a specific station
and ingests them into the climatology_hourly table.

Usage:
  python ingest_normals.py --station KSFO --ncei_id USW00023234
"""

import requests
import pandas as pd
import argparse
from io import StringIO
from lib.db import engine, Base
from lib.weather_model import ClimatologyHourly
from sqlalchemy.orm import Session
from sqlalchemy import delete

def fetch_ncei_normals(ncei_id: str):
    """
    Downloads the hourly normals CSV directly from NCEI.
    """
    url = f"https://www.ncei.noaa.gov/data/normals-hourly/1991-2020/access/{ncei_id}.csv"
    print(f"Downloading normals from: {url}")
    
    response = requests.get(url)
    response.raise_for_status()
    
    # The NCEI CSV has many columns. We only want the core ones.
    df = pd.read_csv(StringIO(response.text), low_memory=False)
    return df

def ingest_normals(df: pd.DataFrame, station_code: str):
    """
    Parses the NCEI dataframe and upserts into climatology_hourly.
    """
    # Create tables if they don't exist
    Base.metadata.create_all(engine)
    
    records = []
    
    # NCEI Columns:
    # DATE: "01-01T00:00:00"
    # HLY-TEMP-NORMAL: Temp in F
    # HLY-DEWP-NORMAL: Dewpoint in F
    # HLY-WIND-AVGSPD: Wind in MPH
    
    print(f"Processing {len(df)} hourly normal records...")
    
    for _, row in df.iterrows():
        try:
            # Parse DATE: "MM-DDT00:00:00"
            date_str = row['DATE']
            month = int(date_str[0:2])
            day = int(date_str[3:5])
            hour = int(date_str[7:9])
            
            # Clean "special" NCEI values (like -9999 or flags)
            def clean_val(val):
                try:
                    f = float(val)
                    return f if f > -100 else None
                except (ValueError, TypeError):
                    return None

            record = {
                "station_code": station_code,
                "month": month,
                "day": day,
                "hour": hour,
                "temp_normal_f": clean_val(row.get('HLY-TEMP-NORMAL')),
                "dewpoint_normal_f": clean_val(row.get('HLY-DEWP-NORMAL')),
                "wind_speed_normal_mph": clean_val(row.get('HLY-WIND-AVGSPD')),
                "precip_prob_pct": clean_val(row.get('HLY-PRCP-PCTPRC-NORMAL'))
            }
            records.append(record)
        except Exception as e:
            continue

    if records:
        print(f"Inserting normals into database...")
        with Session(engine) as s:
            # Clear old normals for this station to avoid duplicates
            s.execute(delete(ClimatologyHourly).where(ClimatologyHourly.station_code == station_code))
            s.bulk_insert_mappings(ClimatologyHourly, records)
            s.commit()
        print("Ingestion complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--station", default="KSFO", help="Internal station code")
    parser.add_argument("--ncei_id", default="USW00023234", help="NCEI Station ID (e.g. USW00023234 for SFO)")
    args = parser.parse_args()
    
    df = fetch_ncei_normals(args.ncei_id)
    ingest_normals(df, args.station)
