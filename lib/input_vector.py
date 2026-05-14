"""
input_vector.py
===============
PID-Enhanced feature encoding: Proportional (Anomaly), Integral (Accumulated), 
and Derivative (Rate of Change) features for stable convergence.
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
from lib.constants import MAX_SEQ_LEN, N_FEATURES

# Weather token normalization: plain English -> METAR code
WEATHER_TOKEN_NORMALIZE = {
    "fog": "FG", "mist": "BR", "haze": "HZ", "rain": "RA",
    "light rain": "-RA", "heavy rain": "+RA", "drizzle": "DZ", 
    "light drizzle": "-DZ", "snow": "SN", "smoke": "FU", "dust": "DU",
    "sand": "SA", "thunderstorm": "TS", "squall": "SQ",
    "thunder": "TS", "lt rain": "-RA", "hvy rain": "+RA",
    "funnel": "FC", "freezing rain": "FZRA", "patches": "BC",
    "mist,fog": ["BR", "FG"], "light rain,mist": ["-RA", "BR"],
    "rain,mist": ["RA", "BR"], "fog,mist": ["FG", "BR"]
}

WEATHER_VOCAB = [
    "RA", "-RA", "+RA", "DZ", "-DZ", "SN", "GR", "GS",
    "FG", "BR", "HZ", "FU", "DU", "SA",
    "TS", "SQ", "FC", "BC", "FZRA", "FZDZ",
    "VCTS", "VCFG", "VCSH"
]
WEATHER_VOCAB_INDEX = {t: i for i, t in enumerate(WEATHER_VOCAB)}
WEATHER_SIZE = len(WEATHER_VOCAB) # 23

CLOUD_COVERAGE_VOCAB = ["CLR", "SKC", "FEW", "SCT", "BKN", "OVC", "VV"]
CLOUD_COVERAGE_INDEX = {c: i for i, c in enumerate(CLOUD_COVERAGE_VOCAB)}
CLOUD_COVERAGE_SIZE = len(CLOUD_COVERAGE_VOCAB) # 7
MAX_CLOUD_LAYERS = 3

# SF Bay Area daylight range (minutes)
DAYLIGHT_MIN_MINUTES = 570.0
DAYLIGHT_MAX_MINUTES = 870.0

def _to_float_safe(val: Any) -> Optional[float]:
    if val is None: return None
    if isinstance(val, (int, float)):
        return float(val) if not math.isnan(val) else None
    
    try:
        s = str(val).strip().replace("<", "").replace(">", "").strip()
        if not s: return None
        
        if "/" in s and " " in s:
            parts = s.split()
            whole = float(parts[0])
            frac_parts = parts[1].split("/")
            return whole + (float(frac_parts[0]) / float(frac_parts[1]))
        elif "/" in s:
            frac_parts = s.split("/")
            return float(frac_parts[0]) / float(frac_parts[1])
            
        return float(s)
    except (ValueError, TypeError):
        return None

def encode_normalized(value, lo: float, hi: float, missing_val: Optional[float] = None) -> List[float]:
    numeric_val = _to_float_safe(value)
    if numeric_val is None:
        return [missing_val if missing_val is not None else 0.5, 1.0]
    clipped = max(lo, min(hi, numeric_val))
    return [(clipped - lo) / (hi - lo), 0.0]

def encode_temp_anomaly(temp_f, clim_normal_f: float, clim_std_f: float = 10.0) -> List[float]:
    numeric_val = _to_float_safe(temp_f)
    if numeric_val is None:
        return [0.0, 1.0]
    return [(numeric_val - clim_normal_f) / clim_std_f, 0.0]

def encode_wind_direction(direction_str: Optional[str]) -> List[float]:
    WIND_DIRECTION_MAP = {
        "N": 0.0, "NNE": 22.5, "NE": 45.0, "ENE": 67.5, "E": 90.0, "ESE": 112.5,
        "SE": 135.0, "SSE": 157.5, "S": 180.0, "SSW": 202.5, "SW": 225.0, "WSW": 247.5,
        "W": 270.0, "WNW": 292.5, "NW": 315.0, "NNW": 337.5,
        "NORTH": 0.0, "EAST": 90.0, "SOUTH": 180.0, "WEST": 270.0,
        "VARIABLE": None, "CALM": None, "VAR": None, "VRB": None
    }
    deg_val = _to_float_safe(direction_str)
    if deg_val is not None:
        rad = 2 * math.pi * deg_val / 360.0
        return [math.sin(rad), math.cos(rad)]

    if not direction_str or str(direction_str).strip() == "":
        return [0.0, 0.0]
    
    clean = str(direction_str).strip().upper().replace("LIGHT ", "")
    degrees = WIND_DIRECTION_MAP.get(clean)
    if degrees is None:
        return [0.0, 0.0]
    rad = 2 * math.pi * degrees / 360.0
    return [math.sin(rad), math.cos(rad)]

def encode_weather_multi_hot(weather_str: Optional[str]) -> List[float]:
    vec = [0.0] * WEATHER_SIZE
    if not weather_str:
        return vec
    
    raw_tokens = str(weather_str).replace("/", ",").split(",")
    for raw in raw_tokens:
        tok = raw.strip().lower()
        norm = WEATHER_TOKEN_NORMALIZE.get(tok, tok.upper())
        if isinstance(norm, list):
            for n in norm:
                if n in WEATHER_VOCAB_INDEX:
                    vec[WEATHER_VOCAB_INDEX[n]] = 1.0
        elif norm in WEATHER_VOCAB_INDEX:
            vec[WEATHER_VOCAB_INDEX[norm]] = 1.0
            
    return vec

def parse_cloud_layer(token: str) -> Tuple[Optional[str], Optional[float]]:
    token = token.strip().upper()
    if not token: return None, None
    if token in ("CLR", "SKC"): return token, 0.0
    
    if token.startswith("VV") and len(token) > 2:
        alt_str = token[2:]
        if alt_str.isdigit(): return "VV", float(alt_str)
        return "VV", None
        
    if len(token) >= 3:
        code = token[:3]
        alt_str = token[3:]
        if code in CLOUD_COVERAGE_INDEX:
            if alt_str.isdigit(): return code, float(alt_str)
            return code, None
            
    return None, None

def encode_clouds_grammar(clouds_str: Optional[str]) -> List[float]:
    features = []
    tokens = str(clouds_str).strip().split() if clouds_str else []
    for i in range(MAX_CLOUD_LAYERS):
        cov_vec = [0.0] * CLOUD_COVERAGE_SIZE
        alt_norm = 0.0
        if i < len(tokens):
            code, alt = parse_cloud_layer(tokens[i])
            if code in CLOUD_COVERAGE_INDEX: cov_vec[CLOUD_COVERAGE_INDEX[code]] = 1.0
            if alt is not None: alt_norm = min(alt / 300.0, 1.0)
        features.extend(cov_vec)
        features.append(alt_norm)
    return features

class ObservationEncoder:
    def __init__(self, area: AreaConfig, tz: datetime.tzinfo = ZoneInfo("America/Los_Angeles")):
        self.area = area
        self.tz = tz

    def encode(self, obs: dict, clim_normal_f: float, day_features: np.ndarray, pid_features: List[float]) -> np.ndarray:
        station_idx = self.area.station_index_map.get(obs.get("station_code"), 0)
        station = self.area.get_station(obs.get("station_code"))

        features = []
        features.extend(encode_temp_anomaly(obs.get("temp_f"), clim_normal_f))
        features.extend(encode_normalized(obs.get("dewpoint_f"), -20.0, 80.0))
        features.extend(encode_normalized(obs.get("rel_humidity_pct"), 0.0, 100.0))
        features.extend(encode_normalized(obs.get("wind_speed_mph"), 0.0, 60.0))
        features.extend(encode_normalized(obs.get("wind_gust_mph"), 0.0, 80.0))
        features.extend(encode_normalized(obs.get("visibility_m"), 0.0, 10.0, missing_val=1.0))
        press = obs.get("pressure_inhg")
        if press is None or (isinstance(press, float) and math.isnan(press)):
            press = obs.get("altimiter_setting_inhg")
        features.extend(encode_normalized(press, 28.0, 31.0))
        
        for key in ["onehr_precip_in", "threehr_precip_in", "sixhr_precip_in"]:
            val = _to_float_safe(obs.get(key))
            if val is None:
                features.extend([0.0, 1.0])
            else:
                norm = math.log1p(max(0.0, val)) / math.log1p(2.0)
                features.extend([min(norm, 1.0), 0.0])
                
        features.extend(encode_wind_direction(obs.get("wind_direction_t")))
        features.extend(self._encode_datetime(obs.get("datetime_dt")))
        features.append(float(station_idx))
        if station:
            features.extend([min(station.distance_from_primary_km / 100.0, 1.0), float(station.is_coastal), min(station.elevation_m / 500.0, 1.0)])
        else:
            features.extend([0.0, 0.0, 0.0])
            
        features.extend(encode_weather_multi_hot(obs.get("weather_t")))
        features.extend(encode_clouds_grammar(obs.get("clouds_t")))
        features.append(float(day_features[0]))
        features.append(float(day_features[1]))
        features.extend(pid_features)

        return np.array(features, dtype=np.float32)

    def _encode_datetime(self, dt: datetime.datetime) -> List[float]:
        if dt is None: return [0.0] * 5
        dt_local = dt.astimezone(self.tz) if dt.tzinfo else dt.replace(tzinfo=datetime.timezone.utc).astimezone(self.tz)
        minute_of_day = dt_local.hour * 60 + dt_local.minute
        doy = dt_local.timetuple().tm_yday
        return [
            math.sin(2 * math.pi * minute_of_day / 1440),
            math.cos(2 * math.pi * minute_of_day / 1440),
            math.sin(2 * math.pi * doy / 365),
            math.cos(2 * math.pi * doy / 365),
            minute_of_day / 1440.0,
        ]

def encode_daylight(daylight_minutes: Optional[float]) -> float:
    if daylight_minutes is None: return 0.5
    norm = (daylight_minutes - DAYLIGHT_MIN_MINUTES) / (DAYLIGHT_MAX_MINUTES - DAYLIGHT_MIN_MINUTES)
    return float(np.clip(norm, 0.0, 1.0))

class SequenceBuilder:
    def __init__(self, area: AreaConfig, session: Session):
        self.area = area
        self.session = session
        self.tz = ZoneInfo("America/Los_Angeles")
        self.encoder = ObservationEncoder(area, tz=self.tz)

    def _get_summary(self, station_code: str, target_date: datetime.date) -> Optional[SummaryFcstModel]:
        dt_midnight = datetime.datetime.combine(target_date, datetime.time.min).replace(tzinfo=datetime.timezone.utc)
        return self.session.scalars(select(SummaryFcstModel).where(
            SummaryFcstModel.station_code == station_code,
            SummaryFcstModel.date_d == dt_midnight
        )).first()

    def _get_hourly_normals(self, station_code: str, target_date: datetime.date) -> np.ndarray:
        stmt = select(ClimatologyHourly).where(
            ClimatologyHourly.station_code == station_code,
            ClimatologyHourly.month == target_date.month,
            ClimatologyHourly.day == target_date.day
        ).order_by(ClimatologyHourly.hour)
        rows = list(self.session.scalars(stmt).all())
        if not rows: return np.full(MAX_SEQ_LEN, 60.0)
        hourly = np.array([r.temp_normal_f for r in rows], dtype=np.float32)
        xp = np.arange(0, 24)
        x_new = np.linspace(0, 23.95, MAX_SEQ_LEN)
        return np.interp(x_new, xp, hourly).astype(np.float32)

    def _get_local_window_utc(self, target_date: datetime.date) -> Tuple[datetime.datetime, datetime.datetime]:
        local_start = datetime.datetime.combine(target_date, datetime.time.min).replace(tzinfo=self.tz)
        local_end = local_start + datetime.timedelta(days=1)
        return local_start.astimezone(datetime.timezone.utc), local_end.astimezone(datetime.timezone.utc)

    def _clean_and_resample_day(self, df_raw: pd.DataFrame, utc_start: datetime.datetime, limit_end: datetime.datetime) -> pd.DataFrame:
        df = df_raw.copy()
        df["datetime_dt"] = pd.to_datetime(df["datetime_dt"])
        if df["datetime_dt"].dt.tz is None: df["datetime_dt"] = df["datetime_dt"].dt.tz_localize("UTC")
        df = df.set_index("datetime_dt").sort_index()
        
        df = df[df.index < limit_end]
        
        freq = "5min"
        full_index = pd.date_range(start=utc_start, end=utc_start + datetime.timedelta(hours=23, minutes=55), freq=freq, name="datetime_dt")
        df = df.reindex(df.index.union(full_index)).sort_index()
        
        smooth_cols = ["temp_f", "dewpoint_f", "rel_humidity_pct", "wind_speed_mph", "pressure_inhg", "altimiter_setting_inhg", "visibility_m"]
        episodic_cols = ["wind_gust_mph", "wind_direction_t", "weather_t", "clouds_t"]
        
        for col in smooth_cols:
            if col in df.columns:
                df[col] = df[col].apply(_to_float_safe)
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df[smooth_cols] = df[smooth_cols].interpolate(method='linear', limit_direction='forward')
        df[episodic_cols] = df[episodic_cols].ffill()
        df["station_code"] = df["station_code"].ffill().bfill()
        if "visibility_m" in df.columns: df["visibility_m"] = df["visibility_m"].fillna(10.0)
        for col in ["onehr_precip_in", "threehr_precip_in", "sixhr_precip_in"]:
            if col in df.columns: df[col] = df[col].fillna(0.0)
            
        return df.reindex(full_index)

    def _calculate_pid_signals(self, clean_df: pd.DataFrame, normals: np.ndarray, effective_end: datetime.datetime) -> List[List[float]]:
        signals = []
        integral = 0.0
        prev_anomaly = 0.0
        for i, (idx, row) in enumerate(clean_df.iterrows()):
            if idx >= effective_end:
                signals.append([0.0, 0.0, 0.0])
                continue
            current_temp = _to_float_safe(row['temp_f'])
            anomaly = (current_temp - normals[i]) / 10.0 if current_temp is not None else 0.0
            integral += anomaly * (5.0 / 60.0)
            derivative = (anomaly - prev_anomaly) / (5.0 / 60.0)
            signals.append([anomaly, float(np.clip(integral / 5.0, -1, 1)), float(np.clip(derivative / 10.0, -1, 1))])
            prev_anomaly = anomaly
        return signals

    def build_context_sequence(self, station_code: str, target_date: datetime.date, history_days: int = 7) -> np.ndarray:
        start_date = target_date - datetime.timedelta(days=history_days)
        utc_midnights = [datetime.datetime.combine(start_date + datetime.timedelta(days=i), datetime.time.min).replace(tzinfo=datetime.timezone.utc) for i in range(history_days)]
        
        stmt = select(SummaryFcstModel).where(
            SummaryFcstModel.station_code == station_code,
            SummaryFcstModel.date_d.in_(utc_midnights)
        ).order_by(SummaryFcstModel.date_d)
        
        summaries = {s.date_d.astimezone(self.tz).date(): s for s in self.session.scalars(stmt).all()}
        seq = np.zeros((history_days, 10), dtype=np.float32)
        for i in range(history_days):
            d = start_date + datetime.timedelta(days=i)
            s = summaries.get(d)
            if s: 
                max_anom = (s.max_temp_f - (s.max_temp_normal or 65.0)) / 10.0 if s.max_temp_f is not None else 0.0
                min_anom = (s.min_temp_f - (s.min_temp_normal or 50.0)) / 10.0 if s.min_temp_f is not None else 0.0
                precip = math.log1p(max(0.0, s.precip_in or 0.0)) / math.log1p(2.0)
                daylight = encode_daylight(s.daylight_minutes)
                seq[i] = [max_anom, min_anom, precip, 0.5, 0.0, 0.0, 0.0, 0.6, 0.5, daylight]
        return seq

    def build_partial_day_sequence(self, station_code: str, target_date: datetime.date, as_of_utc: Optional[datetime.datetime] = None) -> np.ndarray:
        utc_start, utc_end = self._get_local_window_utc(target_date)
        if as_of_utc is None: as_of_utc = datetime.datetime.now(datetime.timezone.utc)
        elif as_of_utc.tzinfo is None: as_of_utc = as_of_utc.replace(tzinfo=datetime.timezone.utc)
        effective_end = min(utc_end, as_of_utc)
        
        stmt = select(WeatherModel).where(
            WeatherModel.station_code == station_code,
            WeatherModel.datetime_dt >= utc_start,
            WeatherModel.datetime_dt < effective_end
        ).order_by(WeatherModel.datetime_dt)
        observations_raw = list(self.session.scalars(stmt).all())
        
        summary = self._get_summary(station_code, target_date)
        if not observations_raw: return np.zeros((MAX_SEQ_LEN, N_FEATURES), dtype=np.float32)
        
        df_raw = pd.DataFrame([{ "station_code": o.station_code, "datetime_dt": o.datetime_dt, "temp_f": o.temp_f, "dewpoint_f": o.dewpoint_f, "rel_humidity_pct": o.rel_humidity_pct, "wind_speed_mph": o.wind_speed_mph, "wind_gust_mph": o.wind_gust_mph, "wind_direction_t": o.wind_direction_t, "visibility_m": o.visibility_m, "pressure_inhg": o.pressure_inhg, "altimiter_setting_inhg": o.altimiter_setting_inhg, "onehr_precip_in": o.onehr_precip_in, "threehr_precip_in": o.threehr_precip_in, "sixhr_precip_in": o.sixhr_precip_in, "weather_t": o.weather_t, "clouds_t": o.clouds_t } for o in observations_raw])
        clean_df = self._clean_and_resample_day(df_raw, utc_start, effective_end)
        
        hourly_normals = self._get_hourly_normals(station_code, target_date)
        pid_signals = self._calculate_pid_signals(clean_df, hourly_normals, effective_end)
        
        day_feat = np.array([encode_daylight(summary.daylight_minutes if summary else None), float(summary.record_proximity or 0.75) if summary else 0.75], dtype=np.float32)
        seq = np.zeros((MAX_SEQ_LEN, N_FEATURES), dtype=np.float32)
        obs_dicts = clean_df.reset_index().to_dict('records')
        
        for i, row in enumerate(obs_dicts[:MAX_SEQ_LEN]):
            if clean_df.index[i] >= effective_end:
                for key in row: 
                    if key not in ("datetime_dt", "station_code"): row[key] = None
            seq[i] = self.encoder.encode(row, hourly_normals[i], day_feat, pid_signals[i])
        return seq

    def iter_training_days(self):
        station_codes = [s.station_code for s in self.area.stations]
        stmt_summaries = select(SummaryFcstModel).where(
            SummaryFcstModel.station_code.in_(station_codes),
            SummaryFcstModel.max_temp_f.is_not(None),
            SummaryFcstModel.min_temp_f.is_not(None)
        ).order_by(SummaryFcstModel.date_d)
        all_summaries = self.session.scalars(stmt_summaries).all()
        summary_map = {(s.station_code, s.date_d.astimezone(self.tz).date()): s for s in all_summaries}
        
        if not all_summaries: return
        min_date, max_date = min(s.date_d.astimezone(self.tz).date() for s in all_summaries), max(s.date_d.astimezone(self.tz).date() for s in all_summaries)
        fetch_start, fetch_end = self._get_local_window_utc(min_date)[0], self._get_local_window_utc(max_date)[1]
        
        stmt_obs = select(WeatherModel).where(
            WeatherModel.station_code.in_(station_codes),
            WeatherModel.datetime_dt >= fetch_start,
            WeatherModel.datetime_dt < fetch_end
        ).order_by(WeatherModel.station_code, WeatherModel.datetime_dt)
        all_obs = self.session.scalars(stmt_obs).all()
        
        df_all = pd.DataFrame([{ "station_code": o.station_code, "datetime_dt": o.datetime_dt, "temp_f": o.temp_f, "dewpoint_f": o.dewpoint_f, "rel_humidity_pct": o.rel_humidity_pct, "wind_speed_mph": o.wind_speed_mph, "wind_gust_mph": o.wind_gust_mph, "wind_direction_t": o.wind_direction_t, "visibility_m": o.visibility_m, "pressure_inhg": o.pressure_inhg, "altimiter_setting_inhg": o.altimiter_setting_inhg, "onehr_precip_in": o.onehr_precip_in, "threehr_precip_in": o.threehr_precip_in, "sixhr_precip_in": o.sixhr_precip_in, "weather_t": o.weather_t, "clouds_t": o.clouds_t } for o in all_obs])
        if df_all.empty: return
        df_all["datetime_dt"] = pd.to_datetime(df_all["datetime_dt"])
        df_all["local_date"] = df_all["datetime_dt"].dt.tz_convert(self.tz).dt.date
        obs_groups = df_all.groupby(["station_code", "local_date"])
        
        stats = {"success": 0, "skipped": 0}
        snapshot_hours = [6, 9, 12, 15, 18, 21, 24]
        
        for (stn, ldate), summary in summary_map.items():
            try: day_df = obs_groups.get_group((stn, ldate))
            except KeyError: continue
            if len(day_df) < 15: continue
            
            utc_start, _ = self._get_local_window_utc(ldate)
            max_norm = float(summary.max_temp_normal or 65.0)
            min_norm = float(summary.min_temp_normal or 50.0)
            hourly_normals = self._get_hourly_normals(stn, ldate)
            day_feat = np.array([encode_daylight(summary.daylight_minutes), float(summary.record_proximity or 0.75)], dtype=np.float32)
            
            for hr in snapshot_hours:
                snapshot_time = utc_start + datetime.timedelta(hours=hr)
                clean_partial_day = self._clean_and_resample_day(day_df, utc_start, snapshot_time)
                pid_signals = self._calculate_pid_signals(clean_partial_day, hourly_normals, snapshot_time)
                day_dicts = clean_partial_day.reset_index().to_dict('records')
                
                seq = np.zeros((MAX_SEQ_LEN, N_FEATURES), dtype=np.float32)
                for i, row in enumerate(day_dicts[:MAX_SEQ_LEN]):
                    if clean_partial_day.index[i] >= snapshot_time:
                        for k in row:
                            if k not in ("datetime_dt", "station_code"): row[k] = None
                    seq[i] = self.encoder.encode(row, hourly_normals[i], day_feat, pid_signals[i])
                
                stats["success"] += 1
                yield (seq, self.build_context_sequence(stn, ldate), self._to_target(summary.max_temp_f, max_norm), self._to_target(summary.min_temp_f, min_norm), {"date": ldate.isoformat(), "hr": hr})
        print(f"[{self.area.name}] Samples generated: {stats['success']} (from {len(summary_map)} days)")

    def _to_target(self, val, norm):
        anomaly = int(round(val - norm))
        idx = int(np.clip(anomaly - self.area.anomaly_bucket_min_f, 0, self.area.temp_buckets - 1))
        target = np.zeros(self.area.temp_buckets, dtype=np.float32)
        target[idx] = 1.0
        return target

    def make_arrays(self) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, List[dict]]:
        X_live, X_context, y_max, y_min, metas = [], [], [], [], []
        for sample in self.iter_training_days():
            X_live.append(sample[0]); X_context.append(sample[1]); y_max.append(sample[2]); y_min.append(sample[3]); metas.append(sample[4])
        return np.stack(X_live), np.stack(X_context), np.stack(y_max), np.stack(y_min), metas
