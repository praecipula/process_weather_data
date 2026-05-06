"""
validate_data.py
================
Sanity checks and linting for the unified weather database.
Identifies physical impossibilities, data gaps, and label mismatches.
Uses timezone-aware boundaries to match local calendar days.
"""

import pandas as pd
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import select, func
from lib.db import session
from lib.weather_model import WeatherModel, SummaryFcstModel
import sys

# Setup logging to both file and stdout
class Logger(object):
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("validation_results.log", "w")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()

sys.stdout = Logger()

def check_label_observation_consistency(station_code: str, threshold_f: float = 3.0):
    """
    Flags days where the official summary high/low is significantly 
    different from any observation recorded that day.
    Uses proper America/Los_Angeles timezone boundaries.
    """
    print(f"\n--- Checking Label Consistency (Threshold: {threshold_f}F, Timezone: America/Los_Angeles) ---")
    
    # We fetch labels first, then query observations for that specific local window
    # This is more precise than a complex SQL JOIN for handling DST shifts
    stmt_labels = select(SummaryFcstModel).where(
        SummaryFcstModel.station_code == station_code
    ).order_by(SummaryFcstModel.date_d)
    
    labels = session.scalars(stmt_labels).all()
    tz = ZoneInfo("America/Los_Angeles")
    
    inconsistent_days = []
    
    for label in labels:
        if label.max_temp_f is None or label.min_temp_f is None:
            continue
            
        # Define local day boundaries
        local_start = label.date_d.replace(tzinfo=tz)
        local_end = local_start + timedelta(days=1)
        
        # Convert boundaries back to UTC for querying the WeatherModel (which is in UTC)
        utc_start = local_start.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
        utc_end = local_end.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
        
        # Check for DST transition
        is_dst_transition = (local_end - local_start) != timedelta(hours=24)
        transition_note = " (DST Transition)" if is_dst_transition else ""

        stmt_obs = select(
            func.max(WeatherModel.temp_f).label("obs_max"),
            func.min(WeatherModel.temp_f).label("obs_min")
        ).where(
            WeatherModel.station_code == station_code,
            WeatherModel.datetime_dt >= utc_start,
            WeatherModel.datetime_dt < utc_end
        )
        
        res = session.execute(stmt_obs).one()
        obs_max, obs_min = res.obs_max, res.obs_min
        
        if obs_max is None or obs_min is None:
            # We already check density elsewhere, but log this for context
            continue

        max_delta = abs(obs_max - label.max_temp_f)
        min_delta = abs(obs_min - label.min_temp_f)
        
        if max_delta > threshold_f or min_delta > threshold_f:
            print(f"Flagged {label.date_d.date()}{transition_note}:")
            if max_delta > threshold_f:
                print(f"  Max Mismatch: Obs={obs_max}F, Label={label.max_temp_f}F (Delta={max_delta:.1f})")
            if min_delta > threshold_f:
                print(f"  Min Mismatch: Obs={obs_min}F, Label={label.min_temp_f}F (Delta={min_delta:.1f})")
            inconsistent_days.append(label.date_d.date())
            
    return inconsistent_days

def check_data_density(station_code: str, expected_obs: int = 24):
    """
    Identifies days with significant gaps in data.
    """
    print(f"\n--- Checking Data Density (Expected: ~{expected_obs} obs/day) ---")
    
    stmt = (
        select(
            func.date(WeatherModel.datetime_dt).label("obs_date"),
            func.count().label("n_obs")
        )
        .where(WeatherModel.station_code == station_code)
        .group_by("obs_date")
        .having(func.count() < (expected_obs * 0.75))
    )
    
    results = session.execute(stmt).all()
    for row in results:
        print(f"Gap Found {row.obs_date}: Only {row.n_obs} observations.")

def check_physical_bounds():
    """Flags values that are meteorologically impossible for SF Area."""
    print("\n--- Checking Physical Bounds ---")
    
    stmt = select(WeatherModel).where(
        (WeatherModel.temp_f < 20) | (WeatherModel.temp_f > 115)
    )
    results = session.scalars(stmt).all()
    for obs in results:
        print(f"Impossible Temp: {obs.station_code} at {obs.datetime_dt}: {obs.temp_f}F")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--station", default="KSFO")
    args = parser.parse_args()
    
    check_physical_bounds()
    check_data_density(args.station, expected_obs=24)
    check_label_observation_consistency(args.station)
