"""
ingest_normals.py
=================
Downloads 1991-2020 hourly climate normals from NCEI for a specific station
and ingests them into the climatology_hourly table.

Also updates the summary_fcst table with daily max/min normals derived from
the hourly data to ensure consistent baselines for the prediction model.

Usage:
  python ingest_normals.py --station KSFO --ncei_id USW00023234
"""

import requests
import pandas as pd
import argparse
from io import StringIO
from lib.db import engine, Base
from lib.weather_model import ClimatologyHourly, SummaryFcstModel
from sqlalchemy.orm import Session
from sqlalchemy import delete, update, func

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
    Parses the NCEI dataframe and upserts into climatology_hourly and summary_fcst.
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
            # Parse DATE: "MM-DDT00:00:00" or similar
            # Robust parsing using regex to handle potential formatting variations
            date_str = str(row['DATE'])
            import re
            match = re.search(r'(\d{2})-(\d{2})T(\d{2})', date_str)
            if not match:
                print(f"  Warning: Could not parse date string: {date_str}")
                continue
                
            month = int(match.group(1))
            day = int(match.group(2))
            hour = int(match.group(3))
            
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
            print(f"Error processing row: {e}")
            continue

    if records:
        print(f"Inserting {len(records)} hourly normals into database...")
        with Session(engine) as s:
            # Clear old normals for this station to avoid duplicates
            del_stmt = delete(ClimatologyHourly).where(ClimatologyHourly.station_code == station_code)
            res = s.execute(del_stmt)
            print(f"  Deleted {res.rowcount} existing hourly normals.")
            s.bulk_insert_mappings(ClimatologyHourly, records)
            s.commit()
            print("  Hourly normals committed.")
        
        # 2. Update summary_fcst with daily max/min normals
        # We aggregate the hourly normals to find the daily high/low normals.
        print(f"Calculating daily summaries from hourly normals...")
        sdf = pd.DataFrame(records)
        daily_stats = sdf.groupby(['month', 'day'])['temp_normal_f'].agg(['max', 'min']).reset_index()

        print(f"Backfilling daily normals into summary_fcst for {station_code}...")
        total_updated = 0
        with Session(engine) as s:
            for _, row in daily_stats.iterrows():
                m, d = int(row['month']), int(row['day'])
                max_n = int(round(row['max'])) if pd.notna(row['max']) else None
                min_n = int(round(row['min'])) if pd.notna(row['min']) else None
                
                if max_n is None or min_n is None:
                    continue
                    
                # Update all records for this month/day (regardless of year)
                stmt = (
                    update(SummaryFcstModel)
                    .where(
                        SummaryFcstModel.station_code == station_code,
                        func.strftime('%m', SummaryFcstModel.date_d) == f"{m:02d}",
                        func.strftime('%d', SummaryFcstModel.date_d) == f"{d:02d}"
                    )
                    .values(
                        max_temp_normal=max_n,
                        min_temp_normal=min_n
                    )
                )
                res = s.execute(stmt)
                total_updated += res.rowcount
            s.commit()
        print(f"Ingestion complete. Total summary records updated: {total_updated}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--station", default="KSFO", help="Internal station code")
    parser.add_argument("--ncei_id", default="USW00023234", help="NCEI Station ID (e.g. USW00023234 for SFO)")
    args = parser.parse_args()
    
    df = fetch_ncei_normals(args.ncei_id)
    ingest_normals(df, args.station)
