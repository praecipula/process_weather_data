"""
ingest_labels.py
================
Fetches historical daily high/low temperatures (labels) and climatological 
normals from the Iowa Environmental Mesonet (IEM) for our 'summary_fcst' table.

Usage:
  python ingest_labels.py --station KSFO --year 2024
"""

import requests
import pandas as pd
import argparse
from datetime import datetime, date
from lib.db import engine
from lib.weather_model import SummaryFcstModel
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session

def fetch_daily_summaries(station: str, start_year: int, end_year: int):
    """
    Downloads daily summaries from IEM ASOS network.
    Includes max/min temp and precip.
    """
    iem_station = station[1:] if station.startswith('K') else station
    
    # IEM Daily Summary API
    base_url = "https://mesonet.agron.iastate.edu/cgi-bin/request/daily.py"
    params = {
        "network": "CA_ASOS", # Adjust if using stations outside California
        "stations": iem_station,
        "year1": start_year,
        "month1": 1,
        "day1": 1,
        "year2": end_year,
        "month2": 12,
        "day2": 31,
        "format": "comma"
    }
    
    print(f"Fetching daily labels for {station} from {start_year} to {end_year}...")
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    
    from io import StringIO
    df = pd.read_csv(StringIO(response.text))
    return df

def safe_float(val):
    """
    Safely converts IEM values to float.
    Handles 'M' (Missing) and 'T' (Trace).
    """
    if pd.isna(val): return None
    s = str(val).strip().upper()
    if s == 'M' or s == '': return None
    if s == 'T': return 0.0001
    try:
        return float(s)
    except ValueError:
        return None

def ingest_labels(df: pd.DataFrame, station_code: str):
    """
    Maps IEM daily data to SummaryFcstModel and upserts to DB.
    """
    records = []
    for _, row in df.iterrows():
        try:
            # Use safe_float for all numeric lookups
            max_t = safe_float(row.get('max_tmpf') or row.get('max_temp_f'))
            min_t = safe_float(row.get('min_tmpf') or row.get('min_temp_f'))
            precip = safe_float(row.get('precip') or row.get('p01i'))
            
            if pd.isna(row['day']): continue
            
            obs_date = datetime.strptime(row['day'], "%Y-%m-%d")
            
            record = {
                "station_code": station_code,
                "date_d": obs_date,
                "max_temp_f": int(max_t) if max_t is not None else None,
                "min_temp_f": int(min_t) if min_t is not None else None,
                "precip_in": precip,
            }
            records.append(record)
        except Exception as e:
            print(f"Error mapping summary row: {e}")
            continue

    if records:
        print(f"Ingesting {len(records)} daily labels for {station_code}...")
        
        # SQLAlchemy Core Insert for 'ON CONFLICT'
        stmt = insert(SummaryFcstModel).values(records)
        # Update temps if we already had a skeleton row for this day
        stmt = stmt.on_conflict_do_update(
            index_elements=['station_code', 'date_d'],
            set_={
                "max_temp_f": stmt.excluded.max_temp_f,
                "min_temp_f": stmt.excluded.min_temp_f,
                "precip_in": stmt.excluded.precip_in
            }
        )
        
        with Session(engine) as s:
            s.execute(stmt)
            s.commit()
        print("Done.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--station", default="KSFO")
    parser.add_argument("--year", type=int, default=2024)
    args = parser.parse_args()
    
    # You can fetch multiple years at once if needed
    df = fetch_daily_summaries(args.station, args.year, args.year)
    ingest_labels(df, args.station)
