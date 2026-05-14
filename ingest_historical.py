"""
ingest_historical.py
====================
Utility to fetch historical METAR data from Iowa Environmental Mesonet (IEM)
and ingest it into our unified 'weather' database.

Usage:
  python ingest_historical.py --station KSFO --year 2024
"""

import requests
import pandas as pd
import argparse
import time
from datetime import datetime, timedelta, timezone
from lib.db import session, engine
from lib.weather_model import WeatherModel
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import select, func

def fetch_iem_data_with_retries(station: str, start_date: datetime, end_date: datetime, max_retries=3):
    iem_station = station[1:] if station.startswith('K') else station
    base_url = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
    params = {
        "station": iem_station,
        "data": "all",
        "year1": start_date.year, "month1": start_date.month, "day1": start_date.day,
        "year2": end_date.year, "month2": end_date.month, "day2": end_date.day,
        "tz": "Etc/UTC", "format": "comma", "latlon": "no", "direct": "no",
        "report_type": ["1", "2"]
    }
    
    for attempt in range(max_retries):
        try:
            print(f"  -> Requesting {start_date.date()} to {end_date.date()} (Attempt {attempt+1})...")
            response = requests.get(base_url, params=params, timeout=60)
            
            if response.status_code == 429:
                wait = (attempt + 1) * 30
                print(f"     Rate limited (429). Waiting {wait}s...")
                time.sleep(wait)
                continue
                
            response.raise_for_status()
            
            from io import StringIO
            return pd.read_csv(StringIO(response.text), skiprows=5, low_memory=False)
        except requests.exceptions.HTTPError as e:
            if response.status_code == 503:
                wait = (attempt + 1) * 10
                print(f"     Server busy (503). Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise e
    return None

def check_month_exists(station_code: str, start_date: datetime, end_date: datetime) -> bool:
    """
    Checks if we already have data for this month. 
    Temporarily disabled threshold to ensure missing month-end gaps are filled.
    """
    return False # Force re-check of all chunks to catch missing days

def safe_float(val):
    if pd.isna(val): return None
    s = str(val).strip().upper()
    if s == 'M' or s == '': return None
    if s == 'T': return 0.0001
    try:
        return float(s)
    except ValueError:
        return None

def map_iem_to_db(df: pd.DataFrame, station_code: str):
    def build_clouds(row):
        layers = []
        for i in range(1, 5):
            cov = row.get(f'skyc{i}')
            alt = safe_float(row.get(f'skyl{i}'))
            if pd.notna(cov) and cov not in ('CLR', 'SKC', 'VV'):
                alt_val = int(alt / 100) if alt is not None else 0
                layers.append(f"{cov}{alt_val:03d}")
            elif cov in ('CLR', 'SKC'):
                layers.append(cov)
            elif cov == 'VV':
                alt_val = int(alt / 100) if alt is not None else 0
                layers.append(f"VV{alt_val:03d}")
        return " ".join(layers) if layers else None

    records = []
    for _, row in df.iterrows():
        try:
            if pd.isna(row['valid']): continue
            
            temp = safe_float(row.get('tmpf'))
            # Filter out rows with no temperature data (low signal for our model)
            if temp is None:
                continue

            drct = safe_float(row.get('drct'))
            wind_dir = str(int(drct)) if drct is not None else None
            
            record = {
                "station_code": station_code,
                "datetime_dt": datetime.strptime(row['valid'], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc),
                "temp_f": temp,
                "dewpoint_f": safe_float(row.get('dwpf')),
                "rel_humidity_pct": int(safe_float(row.get('relh'))) if safe_float(row.get('relh')) is not None else None,
                "wind_direction_t": wind_dir,
                "wind_speed_mph": int(safe_float(row.get('sknt')) * 1.15078) if safe_float(row.get('sknt')) is not None else None,
                "wind_gust_mph": int(safe_float(row.get('gust')) * 1.15078) if safe_float(row.get('gust')) is not None else None,
                "visibility_m": safe_float(row.get('vsby')),
                "altimiter_setting_inhg": safe_float(row.get('alti')),
                "onehr_precip_in": safe_float(row.get('p01i')),
                "weather_t": row['wxcodes'] if pd.notna(row['wxcodes']) else None,
                "clouds_t": build_clouds(row)
            }
            records.append(record)
        except Exception as e:
            continue

    if records:
        print(f"Ingesting {len(records)} records for {station_code}...")
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session
        write_engine = create_engine("sqlite:///weather.db")
        stmt = insert(WeatherModel).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=['station_code', 'datetime_dt'])
        with Session(write_engine) as s:
            s.execute(stmt)
            s.commit()

def fetch_iem_data_chunked(station: str, start_date: datetime, end_date: datetime):
    curr = start_date
    while curr <= end_date:
        # Calculate start of next month
        next_month = (curr.replace(day=28) + timedelta(days=4)).replace(day=1)
        
        # IEM 'year2/month2/day2' is EXCLUSIVE. 
        # To get all of January, we must request up to Feb 1st.
        chunk_end = next_month
        
        # Ensure we don't skip the last day of the requested range due to UTC offset
        # (We need a few hours of the 'next day' UTC to complete the 'current day' PST)
        if chunk_end > end_date:
            chunk_end = end_date + timedelta(days=1)

        if check_month_exists(station, curr, chunk_end):
            print(f"  -> Skipping {curr.date()} to {chunk_end.date()} (Data exists)")
        else:
            df = fetch_iem_data_with_retries(station, curr, chunk_end)
            if df is not None:
                map_iem_to_db(df, station)
                time.sleep(2) # Polite
        
        curr = next_month

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--station", default="KSFO")
    parser.add_argument("--year", type=int, default=2024)
    args = parser.parse_args()
    
    start = datetime(args.year, 1, 1)
    end = datetime(args.year, 12, 31)
    fetch_iem_data_chunked(args.station, start, end)
