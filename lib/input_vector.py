"""
input_vector.py
===============
Multi-Resolution Input Builder.
Outputs:
1. Macro: 24h of 1-hour data.
2. Micro: 120m of 5-min data.
3. Context: 7d of 1-day data.
"""

import math
import datetime
import pandas as pd
import numpy as np
from typing import Optional, List, Tuple, Any
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from zoneinfo import ZoneInfo

from lib.area_config import AreaConfig, Station
from lib.weather_model import WeatherModel, SummaryFcstModel, ClimatologyHourly
from lib.constants import MACRO_SEQ_LEN, MICRO_SEQ_LEN, N_FEATURES

# Selected high-signal tokens
DISRUPTOR_TOKENS = ["FG", "BR", "RA", "TS"]
CLOUD_COVER_TOKENS = ["CLR", "SCT", "BKN", "OVC"]

def _to_float_safe(val: Any) -> Optional[float]:
    if val is None: return None
    if isinstance(val, (int, float)): return float(val) if not math.isnan(val) else None
    try:
        s = str(val).strip().replace("<", "").replace(">", "").strip()
        if not s: return None
        return float(s)
    except (ValueError, TypeError): return None

class ObservationEncoder:
    def __init__(self, area: AreaConfig, tz: datetime.tzinfo = ZoneInfo("America/Los_Angeles")):
        self.area = area
        self.tz = tz

    def encode(self, obs: dict, clim_normal_f: float, pid_features: List[float]) -> np.ndarray:
        # VECTOR (15): [P, I, D, SinT, CosT, Hum, Wind, FG, BR, RA, TS, CLR, SCT, BKN, OVC]
        dt = obs.get("datetime_dt")
        if dt is None: return np.zeros(15, dtype=np.float32)
        
        dt_local = dt.astimezone(self.tz) if dt.tzinfo else dt.replace(tzinfo=datetime.timezone.utc).astimezone(self.tz)
        min_day = dt_local.hour * 60 + dt_local.minute
        
        features = [
            pid_features[0], # P
            pid_features[1], # I
            pid_features[2], # D
            math.sin(2 * math.pi * min_day / 1440),
            math.cos(2 * math.pi * min_day / 1440),
            (_to_float_safe(obs.get("rel_humidity_pct")) or 50.0) / 100.0,
            min((_to_float_safe(obs.get("wind_speed_mph")) or 0.0) / 40.0, 1.0)
        ]
        
        w_t = str(obs.get("weather_t") or "").upper()
        for tok in DISRUPTOR_TOKENS:
            features.append(1.0 if tok in w_t else 0.0)
            
        c_t = str(obs.get("clouds_t") or "").upper()
        for tok in CLOUD_COVER_TOKENS:
            features.append(1.0 if tok in c_t else 0.0)
            
        return np.array(features, dtype=np.float32)

class SequenceBuilder:
    def __init__(self, area: AreaConfig, session: Session):
        self.area = area
        self.session = session
        self.tz = ZoneInfo("America/Los_Angeles")
        self.encoder = ObservationEncoder(area, tz=self.tz)

    def _get_summary(self, station_code: str, target_date: datetime.date) -> Optional[SummaryFcstModel]:
        dt_midnight = datetime.datetime.combine(target_date, datetime.time.min).replace(tzinfo=datetime.timezone.utc)
        return self.session.scalars(select(SummaryFcstModel).where(SummaryFcstModel.station_code == station_code, SummaryFcstModel.date_d == dt_midnight)).first()

    def _get_hourly_normals(self, station_code: str, target_date: datetime.date, seq_len: int) -> np.ndarray:
        stmt = select(ClimatologyHourly).where(
            ClimatologyHourly.station_code == station_code,
            ClimatologyHourly.month == target_date.month,
            ClimatologyHourly.day == target_date.day
        ).order_by(ClimatologyHourly.hour)
        rows = list(self.session.scalars(stmt).all())
        if not rows: return np.full(seq_len, 60.0)
        hourly = np.array([r.temp_normal_f for r in rows], dtype=np.float32)
        return np.interp(np.linspace(0, 23.999, seq_len), np.arange(0, 24), hourly).astype(np.float32)

    def _get_local_window_utc(self, target_date: datetime.date) -> Tuple[datetime.datetime, datetime.datetime]:
        local_start = datetime.datetime.combine(target_date, datetime.time.min).replace(tzinfo=self.tz)
        local_end = local_start + datetime.timedelta(days=1)
        return local_start.astimezone(datetime.timezone.utc), local_end.astimezone(datetime.timezone.utc)

    def _clean_and_resample(self, df_raw: pd.DataFrame, start_utc: datetime.datetime, end_utc: datetime.datetime, freq: str) -> pd.DataFrame:
        df = df_raw.copy()
        df["datetime_dt"] = pd.to_datetime(df["datetime_dt"])
        if df["datetime_dt"].dt.tz is None: df["datetime_dt"] = df["datetime_dt"].dt.tz_localize("UTC")
        df = df.set_index("datetime_dt").sort_index()
        df = df[df.index < end_utc]
        full_index = pd.date_range(start=start_utc, end=end_utc - datetime.timedelta(minutes=1), freq=freq, name="datetime_dt")
        df = df.reindex(df.index.union(full_index)).sort_index()
        
        for col in ['temp_f', 'rel_humidity_pct', 'wind_speed_mph']:
            df[col] = df[col].apply(_to_float_safe)
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df[['temp_f', 'rel_humidity_pct', 'wind_speed_mph']] = df[['temp_f', 'rel_humidity_pct', 'wind_speed_mph']].interpolate(method='linear', limit_direction='forward')
        df[['weather_t', 'clouds_t']] = df[['weather_t', 'clouds_t']].ffill()
        return df.reindex(full_index)

    def _calculate_pid_signals(self, clean_df: pd.DataFrame, normals: np.ndarray, dt_step_minutes: float) -> List[List[float]]:
        signals = []
        integral, prev_anomaly = 0.0, 0.0
        for i, (idx, row) in enumerate(clean_df.iterrows()):
            current_temp = row['temp_f']
            anomaly = (current_temp - normals[i]) / 10.0 if current_temp is not None else 0.0
            integral += anomaly * (dt_step_minutes / 60.0)
            derivative = (anomaly - prev_anomaly) / (dt_step_minutes / 60.0)
            signals.append([anomaly, float(np.clip(integral / 5.0, -1, 1)), float(np.clip(derivative / 10.0, -1, 1))])
            prev_anomaly = anomaly
        return signals

    def build_context_sequence(self, station_code: str, target_date: datetime.date) -> np.ndarray:
        start_date = target_date - datetime.timedelta(days=7)
        utc_midnights = [datetime.datetime.combine(start_date + datetime.timedelta(days=i), datetime.time.min).replace(tzinfo=datetime.timezone.utc) for i in range(7)]
        summaries = {s.date_d.astimezone(self.tz).date(): s for s in self.session.scalars(select(SummaryFcstModel).where(SummaryFcstModel.station_code == station_code, SummaryFcstModel.date_d.in_(utc_midnights))).all()}
        seq = np.zeros((7, 1), dtype=np.float32)
        for i in range(7):
            d = start_date + datetime.timedelta(days=i)
            if d in summaries:
                s = summaries[d]
                seq[i] = [(s.max_temp_f-(s.max_temp_normal or 65.0))/10.0 if s.max_temp_f else 0]
        return seq

    def build_multi_day_sequence(self, station_code: str, target_date: datetime.date, as_of_utc: Optional[datetime.datetime] = None) -> Tuple[np.ndarray, np.ndarray]:
        utc_start, utc_end = self._get_local_window_utc(target_date)
        if as_of_utc is None: as_of_utc = datetime.datetime.now(datetime.timezone.utc)
        elif as_of_utc.tzinfo is None: as_of_utc = as_of_utc.replace(tzinfo=datetime.timezone.utc)
        effective_end = min(utc_end, as_of_utc)
        
        obs_raw = self.session.scalars(select(WeatherModel).where(WeatherModel.station_code == station_code, WeatherModel.datetime_dt >= utc_start, WeatherModel.datetime_dt < effective_end).order_by(WeatherModel.datetime_dt)).all()
        if not obs_raw:
            return np.zeros((MACRO_SEQ_LEN, N_FEATURES), dtype=np.float32), np.zeros((MICRO_SEQ_LEN, N_FEATURES), dtype=np.float32)
            
        df_raw = pd.DataFrame([{ "datetime_dt": o.datetime_dt, "temp_f": o.temp_f, "rel_humidity_pct": o.rel_humidity_pct, "wind_speed_mph": o.wind_speed_mph, "weather_t": o.weather_t, "clouds_t": o.clouds_t } for o in obs_raw])
        
        macro_df = self._clean_and_resample(df_raw, utc_start, effective_end, freq="1h")
        macro_normals = self._get_hourly_normals(station_code, target_date, len(macro_df))
        macro_pid = self._calculate_pid_signals(macro_df, macro_normals, dt_step_minutes=60.0)
        macro_seq = np.zeros((MACRO_SEQ_LEN, N_FEATURES), dtype=np.float32)
        for i, row in enumerate(macro_df.reset_index().to_dict('records')[:MACRO_SEQ_LEN]):
            macro_seq[i] = self.encoder.encode(row, macro_normals[i], macro_pid[i])

        micro_start = effective_end - datetime.timedelta(minutes=120)
        micro_start = max(utc_start, micro_start) 
        micro_df = self._clean_and_resample(df_raw, micro_start, effective_end, freq="5min")
        micro_full_idx = pd.date_range(start=effective_end - datetime.timedelta(minutes=120), end=effective_end - datetime.timedelta(minutes=1), freq="5min")
        micro_df = micro_df.reindex(micro_full_idx).ffill() 
        micro_normals = self._get_hourly_normals(station_code, target_date, len(micro_df)) 
        micro_pid = self._calculate_pid_signals(micro_df, micro_normals, dt_step_minutes=5.0)
        micro_seq = np.zeros((MICRO_SEQ_LEN, N_FEATURES), dtype=np.float32)
        for i, row in enumerate(micro_df.reset_index().to_dict('records')[:MICRO_SEQ_LEN]):
            micro_seq[i] = self.encoder.encode(row, micro_normals[i], micro_pid[i])
            
        return macro_seq, micro_seq

    def get_total_days(self) -> int:
        station_codes = [s.station_code for s in self.area.stations]
        return self.session.execute(
            select(func.count())
            .select_from(SummaryFcstModel)
            .where(
                SummaryFcstModel.station_code.in_(station_codes),
                SummaryFcstModel.max_temp_f.is_not(None),
                SummaryFcstModel.min_temp_f.is_not(None)
            )
        ).scalar() or 0

    def iter_training_days(self):
        station_codes = [s.station_code for s in self.area.stations]
        all_summaries = self.session.scalars(select(SummaryFcstModel).where(SummaryFcstModel.station_code.in_(station_codes), SummaryFcstModel.max_temp_f.is_not(None), SummaryFcstModel.min_temp_f.is_not(None)).order_by(SummaryFcstModel.date_d)).all()
        summary_map = {(s.station_code, s.date_d.astimezone(self.tz).date()): s for s in all_summaries}
        if not all_summaries: return
        min_date, max_date = min(s.date_d.astimezone(self.tz).date() for s in all_summaries), max(s.date_d.astimezone(self.tz).date() for s in all_summaries)
        fetch_start, fetch_end = self._get_local_window_utc(min_date)[0], self._get_local_window_utc(max_date)[1]
        
        all_obs = self.session.scalars(select(WeatherModel).where(WeatherModel.station_code.in_(station_codes), WeatherModel.datetime_dt >= fetch_start, WeatherModel.datetime_dt < fetch_end).order_by(WeatherModel.station_code, WeatherModel.datetime_dt)).all()
        df_all = pd.DataFrame([{ "station_code": o.station_code, "datetime_dt": o.datetime_dt, "temp_f": o.temp_f, "rel_humidity_pct": o.rel_humidity_pct, "wind_speed_mph": o.wind_speed_mph, "weather_t": o.weather_t, "clouds_t": o.clouds_t } for o in all_obs])
        if df_all.empty: return
        df_all["datetime_dt"] = pd.to_datetime(df_all["datetime_dt"])
        df_all["local_date"] = df_all["datetime_dt"].dt.tz_convert(self.tz).dt.date
        obs_groups = df_all.groupby(["station_code", "local_date"])
        
        snapshot_hours = [6, 9, 12, 15, 18, 21, 24]
        for (stn, ldate), summary in summary_map.items():
            try: day_df = obs_groups.get_group((stn, ldate))
            except KeyError: continue
            if len(day_df) < 15: continue
            
            utc_start, _ = self._get_local_window_utc(ldate)
            max_norm, min_norm = float(summary.max_temp_normal or 65.0), float(summary.min_temp_normal or 50.0)
            
            day_samples = []
            for hr in snapshot_hours:
                snapshot_time = utc_start + datetime.timedelta(hours=hr)
                
                macro_df = self._clean_and_resample(day_df, utc_start, snapshot_time, freq="1h")
                macro_normals = self._get_hourly_normals(stn, ldate, len(macro_df))
                macro_pid = self._calculate_pid_signals(macro_df, macro_normals, dt_step_minutes=60.0)
                macro_seq = np.zeros((MACRO_SEQ_LEN, N_FEATURES), dtype=np.float32)
                for i, row in enumerate(macro_df.reset_index().to_dict('records')[:MACRO_SEQ_LEN]):
                    macro_seq[i] = self.encoder.encode(row, macro_normals[i], macro_pid[i])

                micro_start = snapshot_time - datetime.timedelta(minutes=120)
                micro_start = max(utc_start, micro_start)
                micro_df = self._clean_and_resample(day_df, micro_start, snapshot_time, freq="5min")
                micro_full_idx = pd.date_range(start=snapshot_time - datetime.timedelta(minutes=120), end=snapshot_time - datetime.timedelta(minutes=1), freq="5min")
                micro_df = micro_df.reindex(micro_full_idx).ffill()
                micro_normals = self._get_hourly_normals(stn, ldate, len(micro_df))
                micro_pid = self._calculate_pid_signals(micro_df, micro_normals, dt_step_minutes=5.0)
                micro_seq = np.zeros((MICRO_SEQ_LEN, N_FEATURES), dtype=np.float32)
                for i, row in enumerate(micro_df.reset_index().to_dict('records')[:MICRO_SEQ_LEN]):
                    micro_seq[i] = self.encoder.encode(row, micro_normals[i], micro_pid[i])
                    
                ctx_seq = self.build_context_sequence(stn, ldate)
                
                day_samples.append((macro_seq, micro_seq, ctx_seq, self._to_target(summary.max_temp_f, max_norm), self._to_target(summary.min_temp_f, min_norm), {"date": ldate.isoformat(), "hr": hr}))
            
            if day_samples:
                yield day_samples

    def _to_target(self, val, norm):
        anomaly = int(round(val - norm))
        idx = int(np.clip(anomaly - self.area.anomaly_bucket_min_f, 0, self.area.temp_buckets - 1))
        target = np.zeros(self.area.temp_buckets, dtype=np.float32)
        target[idx] = 1.0
        return target

    def make_arrays(self) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, List[dict]]:
        X_macro, X_micro, X_context, y_max, y_min, metas = [], [], [], [], [], []
        for sample in self.iter_training_days():
            X_macro.append(sample[0])
            X_micro.append(sample[1])
            X_context.append(sample[2])
            y_max.append(sample[3])
            y_min.append(sample[4])
            metas.append(sample[5])
        return np.stack(X_macro), np.stack(X_micro), np.stack(X_context), np.stack(y_max), np.stack(y_min), metas
