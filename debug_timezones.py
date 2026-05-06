
import datetime
from sqlalchemy import select, func
from lib.db import session
from lib.weather_model import WeatherModel, SummaryFcstModel
import sys

# Setup logging to both file and stdout
class Logger(object):
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("debug_timezones.log", "w")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()

sys.stdout = Logger()

def compare_boundaries(station_code, target_date_str):
    target_date = datetime.datetime.fromisoformat(target_date_str).date()
    print(f"--- Analyzing {station_code} on {target_date} ---")
    
    # 1. Get official label
    stmt_label = select(SummaryFcstModel).where(
        SummaryFcstModel.station_code == station_code,
        SummaryFcstModel.date_d == datetime.datetime(target_date.year, target_date.month, target_date.day)
    )
    label = session.scalars(stmt_label).first()
    if not label:
        print("No label found.")
        return
    
    print(f"Official Label: Max={label.max_temp_f}F, Min={label.min_temp_f}F")

    # 2. Get observations for the window (+/- 1 day to be safe)
    start_search = datetime.datetime(target_date.year, target_date.month, target_date.day) - datetime.timedelta(days=1)
    end_search = datetime.datetime(target_date.year, target_date.month, target_date.day) + datetime.timedelta(days=2)
    
    stmt_obs = select(WeatherModel).where(
        WeatherModel.station_code == station_code,
        WeatherModel.datetime_dt >= start_search,
        WeatherModel.datetime_dt < end_search
    ).order_by(WeatherModel.datetime_dt)
    
    obs_list = list(session.scalars(stmt_obs).all())
    
    # 3. Calculate Max/Min using UTC group
    # (Simulating what validate_data.py does)
    utc_temps = [o.temp_f for o in obs_list if o.datetime_dt.date() == target_date and o.temp_f is not None]
    if utc_temps:
        print(f"UTC Window (Midnight-Midnight UTC): Max={max(utc_temps)}F, Min={min(utc_temps)}F")
    else:
        print("No UTC observations found.")

    # 4. Calculate Max/Min using Local Window (UTC-8)
    # Most anomalous dates are in winter (PST = UTC-8)
    local_offset = datetime.timedelta(hours=-8)
    local_temps = []
    for o in obs_list:
        if o.temp_f is None: continue
        local_dt = o.datetime_dt + local_offset
        if local_dt.date() == target_date:
            local_temps.append(o.temp_f)
            
    if local_temps:
        print(f"Local Window (Midnight-Midnight PST): Max={max(local_temps)}F, Min={min(local_temps)}F")
        
    print("\nConclusion:")
    if local_temps and max(local_temps) == label.max_temp_f and min(local_temps) == label.min_temp_f:
        print("SUCCESS: Local window matches labels exactly!")
    else:
        print("STILL MISMATCHED: Offset might be different (PDT vs PST) or data is just noisy.")

if __name__ == "__main__":
    # Test a few anomalous days from validate_data results
    compare_boundaries("KSFO", "2023-02-21") # 7F delta in max
    print("\n" + "="*40 + "\n")
    compare_boundaries("KSFO", "2023-01-09") # 5F delta in min
    print("\n" + "="*40 + "\n")
    compare_boundaries("KSFO", "2024-01-01") # 9F delta in max
