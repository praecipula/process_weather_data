
import datetime
from sqlalchemy import select
from lib.db import session
from lib.weather_model import WeatherModel, SummaryFcstModel
import sys

# Setup logging to both file and stdout
class Logger(object):
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("debug_outlier.log", "w")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()

sys.stdout = Logger()

def inspect_range(station_code, target_date, days_around=2):
    print(f"--- Inspecting {station_code} around {target_date} (+/- {days_around} days) ---")
    
    start = datetime.datetime(target_date.year, target_date.month, target_date.day) - datetime.timedelta(days=days_around)
    end = datetime.datetime(target_date.year, target_date.month, target_date.day) + datetime.timedelta(days=days_around + 1)
    
    # Get Labels for the range
    stmt_label = select(SummaryFcstModel).where(
        SummaryFcstModel.station_code == station_code,
        SummaryFcstModel.date_d >= start,
        SummaryFcstModel.date_d < end
    ).order_by(SummaryFcstModel.date_d)
    
    summaries = session.scalars(stmt_label).all()
    print("\nLabels in range:")
    for s in summaries:
        print(f"  {s.date_d.date()}: Max={s.max_temp_f}F, Min={s.min_temp_f}F")

    # Get Observations for the range
    stmt_obs = select(WeatherModel).where(
        WeatherModel.station_code == station_code,
        WeatherModel.datetime_dt >= start,
        WeatherModel.datetime_dt < end
    ).order_by(WeatherModel.datetime_dt)
    
    obs_list = list(session.scalars(stmt_obs).all())
    print(f"\nTotal Observations in range: {len(obs_list)}")
    
    if obs_list:
        # Find absolute max in this range
        temps = [(o.datetime_dt, o.temp_f) for o in obs_list if o.temp_f is not None]
        if temps:
            max_obs_dt, max_obs_val = max(temps, key=lambda x: x[1])
            min_obs_dt, min_obs_val = min(temps, key=lambda x: x[1])
            print(f"Absolute Max in range: {max_obs_val}F at {max_obs_dt.isoformat()}")
            print(f"Absolute Min in range: {min_obs_val}F at {min_obs_dt.isoformat()}")
            
            # Print daily maxes in UTC
            curr = start
            while curr < end:
                d_start = curr
                d_end = curr + datetime.timedelta(days=1)
                day_temps = [t for dt, t in temps if d_start <= dt < d_end]
                if day_temps:
                    print(f"  UTC {d_start.date()}: Max={max(day_temps)}F")
                curr = d_end

if __name__ == "__main__":
    inspect_range("KSFO", datetime.date(2024, 1, 1), days_around=2)
