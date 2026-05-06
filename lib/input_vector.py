"""
input_vector.py
===============
Feature encoding pipeline: WeatherModel rows -> normalized numpy vectors.

This version implements sophisticated parsers for METAR-style weather and 
cloud strings to capture simultaneous conditions and vertical cloud structure.
"""

import math
import datetime
import pandas as pd
import numpy as np
from typing import Optional, List, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from zoneinfo import ZoneInfo

from lib.area_config import AreaConfig, Station
from lib.weather_model import WeatherModel, SummaryFcstModel


from lib.constants import MAX_SEQ_LEN, N_FEATURES

# Weather token normalization: plain English -> METAR code
# This ensures consistency across different data sources (NWS vs Wunderground)
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


# =============================================================================
# PRIMITIVE ENCODERS
# =============================================================================

def encode_normalized(value, lo: float, hi: float,
                       missing_val: Optional[float] = None) -> List[float]:
    """Returns [normalized_value, missing_flag]"""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return [missing_val if missing_val is not None else 0.0, 1.0]
    clipped = max(lo, min(hi, float(value)))
    return [(clipped - lo) / (hi - lo), 0.0]


def encode_temp_anomaly(temp_f, clim_normal_f: float,
                         clim_std_f: float = 10.0) -> List[float]:
    """Returns [anomaly_normalized, missing_flag]"""
    if temp_f is None or (isinstance(temp_f, float) and math.isnan(temp_f)):
        return [0.0, 1.0]
    return [(float(temp_f) - clim_normal_f) / clim_std_f, 0.0]


def encode_wind_direction(direction_str: Optional[str]) -> List[float]:
    """Circular encoding for wind direction. Returns [sin, cos]."""
    WIND_DIRECTION_MAP = {
        "N": 0.0, "NNE": 22.5, "NE": 45.0, "ENE": 67.5, "E": 90.0, "ESE": 112.5,
        "SE": 135.0, "SSE": 157.5, "S": 180.0, "SSW": 202.5, "SW": 225.0, "WSW": 247.5,
        "W": 270.0, "WNW": 292.5, "NW": 315.0, "NNW": 337.5,
        "NORTH": 0.0, "EAST": 90.0, "SOUTH": 180.0, "WEST": 270.0,
        "VARIABLE": None, "CALM": None, "VAR": None, "VRB": None
    }
    if not direction_str or str(direction_str).strip() == "":
        return [0.0, 0.0]
    
    # Simple cleanup for common variations
    clean = str(direction_str).strip().upper().replace("LIGHT ", "")
    degrees = WIND_DIRECTION_MAP.get(clean)
    if degrees is None:
        return [0.0, 0.0]
    rad = 2 * math.pi * degrees / 360.0
    return [math.sin(rad), math.cos(rad)]


def encode_weather_multi_hot(weather_str: Optional[str]) -> List[float]:
    """Parses comma or slash separated weather tokens into multi-hot vector."""
    vec = [0.0] * WEATHER_SIZE
    if not weather_str:
        return vec
    
    # Handle separators: "," or "/"
    raw_tokens = str(weather_str).replace("/", ",").split(",")
    for raw in raw_tokens:
        tok = raw.strip().lower()
        norm = WEATHER_TOKEN_NORMALIZE.get(tok, tok.upper())
        
        # Mapping might return a list (e.g. "mist,fog")
        if isinstance(norm, list):
            for n in norm:
                if n in WEATHER_VOCAB_INDEX:
                    vec[WEATHER_VOCAB_INDEX[n]] = 1.0
        elif norm in WEATHER_VOCAB_INDEX:
            vec[WEATHER_VOCAB_INDEX[norm]] = 1.0
            
    return vec


def parse_cloud_layer(token: str) -> Tuple[Optional[str], Optional[float]]:
    """
    Parses a single METAR cloud token like 'FEW017' or 'OVC028'.
    Returns (coverage_code, altitude_hundreds_of_feet).
    """
    token = token.strip().upper()
    if not token:
        return None, None
    
    if token in ("CLR", "SKC"):
        return token, 0.0
    
    # Vertical Visibility
    if token.startswith("VV") and len(token) > 2:
        alt_str = token[2:]
        if alt_str.isdigit():
            return "VV", float(alt_str)
        return "VV", None
        
    # Standard layers: COVERAGE + ALTITUDE (e.g. BKN025)
    if len(token) >= 3:
        code = token[:3]
        alt_str = token[3:]
        if code in CLOUD_COVERAGE_INDEX:
            if alt_str.isdigit():
                return code, float(alt_str)
            return code, None
            
    return None, None


def encode_clouds_grammar(clouds_str: Optional[str]) -> List[float]:
    """
    Parses 'FEW017 OVC028' style grammar. 
    Encodes up to 3 layers. Each layer = 7 (multi-hot coverage) + 1 (norm altitude).
    Total = 8 * 3 = 24 features.
    """
    features = []
    tokens = str(clouds_str).strip().split() if clouds_str else []
    
    for i in range(MAX_CLOUD_LAYERS):
        cov_vec = [0.0] * CLOUD_COVERAGE_SIZE
        alt_norm = 0.0
        
        if i < len(tokens):
            code, alt = parse_cloud_layer(tokens[i])
            if code in CLOUD_COVERAGE_INDEX:
                cov_vec[CLOUD_COVERAGE_INDEX[code]] = 1.0
            if alt is not None:
                # Normalize altitude: cap at 30,000 ft (300 units)
                alt_norm = min(alt / 300.0, 1.0)
        
        features.extend(cov_vec)
        features.append(alt_norm)
        
    return features


# =============================================================================
# OBSERVATION ENCODER
# =============================================================================

class ObservationEncoder:
    def __init__(self, area: AreaConfig):
        self.area = area

    def encode(self, obs: dict,
               clim_normal_f: float,
               day_features: np.ndarray) -> np.ndarray:
        """
        Modified to accept a dictionary (Pandas row) instead of WeatherModel 
        to bypass the slow SQL-based decorators.
        """
        station_idx = self.area.station_index_map.get(obs.get("station_code"), 0)
        station = self.area.get_station(obs.get("station_code"))

        features = []

        # 0-1: Temperature anomaly
        features.extend(encode_temp_anomaly(obs.get("temp_f"), clim_normal_f))
        
        # 2-3: Dewpoint (Range -20 to 80)
        features.extend(encode_normalized(obs.get("dewpoint_f"), -20.0, 80.0))
        
        # 4-5: Humidity (0-100)
        features.extend(encode_normalized(obs.get("rel_humidity_pct"), 0.0, 100.0))
        
        # 6-7: Wind Speed (0-60)
        features.extend(encode_normalized(obs.get("wind_speed_mph"), 0.0, 60.0))
        
        # 8-9: Wind Gust (0-80)
        features.extend(encode_normalized(obs.get("wind_gust_mph"), 0.0, 80.0))
        
        # 10-11: Visibility (0-10 miles, missing defaults to clear)
        features.extend(encode_normalized(obs.get("visibility_m"), 0.0, 10.0, missing_val=1.0))
        
        # 12-13: Pressure (28 to 31 inHg)
        # Composite pressure: use pressure_inhg or fallback to altimeter
        press = obs.get("pressure_inhg")
        if press is None or math.isnan(press):
            press = obs.get("altimiter_setting_inhg")
        features.extend(encode_normalized(press, 28.0, 31.0))
        
        # 14-19: Precip (1, 3, 6 hr)
        from lib.input_vector import encode_precip
        features.extend(encode_precip(obs.get("onehr_precip_in")))
        features.extend(encode_precip(obs.get("threehr_precip_in")))
        features.extend(encode_precip(obs.get("sixhr_precip_in")))
        
        # 20-21: Wind Direction sin/cos
        features.extend(encode_wind_direction(obs.get("wind_direction_t")))
        
        # 22-26: Datetime circular + fraction
        from lib.input_vector import encode_datetime
        features.extend(encode_datetime(obs.get("datetime_dt")))
        
        # 27: Station Index (Categorical)
        features.append(float(station_idx))
        
        # 28-30: Station Geo
        if station:
            from lib.input_vector import encode_station_geo
            features.extend(encode_station_geo(station).tolist())
        else:
            features.extend([0.0, 0.0, 0.0])
            
        # 31-53: Weather Multi-hot (23 tokens)
        features.extend(encode_weather_multi_hot(obs.get("weather_t")))
        
        # 54-77: Cloud Grammar
        features.extend(encode_clouds_grammar(obs.get("clouds_t")))
        
        # 78-79: Daylight & Record Proximity
        features.append(float(day_features[0]))
        features.append(float(day_features[1]))

        return np.array(features, dtype=np.float32)

# =============================================================================
# SEQUENCE BUILDER & HELPERS
# =============================================================================

def encode_precip(value) -> List[float]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return [0.0, 1.0]
    norm = math.log1p(max(0.0, float(value))) / math.log1p(2.0)
    return [min(norm, 1.0), 0.0]

def encode_datetime(dt: datetime.datetime) -> List[float]:
    minute_of_day = dt.hour * 60 + dt.minute
    doy = dt.timetuple().tm_yday
    return [
        math.sin(2 * math.pi * minute_of_day / 1440),
        math.cos(2 * math.pi * minute_of_day / 1440),
        math.sin(2 * math.pi * doy / 365),
        math.cos(2 * math.pi * doy / 365),
        minute_of_day / 1440.0,
    ]

def encode_station_geo(station: Station) -> np.ndarray:
    return np.array([
        min(station.distance_from_primary_km / 100.0, 1.0),
        float(station.is_coastal),
        min(station.elevation_m / 500.0, 1.0),
    ], dtype=np.float32)

def encode_daylight(daylight_minutes: Optional[float]) -> float:
    if daylight_minutes is None: return 0.5
    norm = (daylight_minutes - DAYLIGHT_MIN_MINUTES) / (DAYLIGHT_MAX_MINUTES - DAYLIGHT_MIN_MINUTES)
    return float(np.clip(norm, 0.0, 1.0))


class SequenceBuilder:
    def __init__(self, area: AreaConfig, session: Session):
        self.area = area
        self.session = session
        self.encoder = ObservationEncoder(area)
        self.tz = ZoneInfo("America/Los_Angeles")

    def _get_local_window_utc(self, target_date: datetime.date) -> Tuple[datetime.datetime, datetime.datetime]:
        local_start = datetime.datetime.combine(target_date, datetime.time.min).replace(tzinfo=self.tz)
        local_end = local_start + datetime.timedelta(days=1)
        utc_start = local_start.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        utc_end = local_end.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return utc_start, utc_end

    def _is_dst_transition(self, target_date: datetime.date) -> bool:
        local_start = datetime.datetime.combine(target_date, datetime.time.min).replace(tzinfo=self.tz)
        local_end = local_start + datetime.timedelta(days=1)
        return (local_end - local_start) != datetime.timedelta(hours=24)

    def _anomaly_to_target(self, actual_f: float, normal_f: float) -> np.ndarray:
        anomaly = int(round(actual_f - normal_f))
        idx = anomaly - self.area.anomaly_bucket_min_f
        idx = int(np.clip(idx, 0, self.area.temp_buckets - 1))
        target = np.zeros(self.area.temp_buckets, dtype=np.float32)
        target[idx] = 1.0
        return target

    def build_day_sequence_from_data(self, observations: List[dict], summary: Optional[SummaryFcstModel]) -> Optional[np.ndarray]:
        if not observations: return None
        
        # Safe access to summary fields with defaults
        norm_f = 65.0
        daylight = None
        proximity = 0.75
        
        if summary:
            if summary.max_temp_normal is not None:
                norm_f = float(summary.max_temp_normal)
            daylight = summary.daylight_minutes
            if summary.record_proximity is not None:
                proximity = float(summary.record_proximity)
                
        day_feat = np.array([encode_daylight(daylight), proximity], dtype=np.float32)
        
        seq = np.zeros((MAX_SEQ_LEN, N_FEATURES), dtype=np.float32)
        for i, obs in enumerate(observations[:MAX_SEQ_LEN]):
            seq[i] = self.encoder.encode(obs, norm_f, day_feat)
        return seq

    def iter_training_days(self, consistency_threshold: float = 5.0):
        station_codes = [s.station_code for s in self.area.stations]
        
        # 1. Bulk Fetch Summaries
        print(f"[{self.area.name}] Bulk fetching summaries...")
        stmt_summaries = select(SummaryFcstModel).where(
            SummaryFcstModel.station_code.in_(station_codes),
            SummaryFcstModel.max_temp_f.is_not(None),
            SummaryFcstModel.min_temp_f.is_not(None)
        ).order_by(SummaryFcstModel.date_d)
        all_summaries = self.session.scalars(stmt_summaries).all()
        summary_map = {(s.station_code, s.date_d.date()): s for s in all_summaries}
        
        if not all_summaries:
            return

        min_date = min(s.date_d.date() for s in all_summaries)
        max_date = max(s.date_d.date() for s in all_summaries)
        fetch_start, _ = self._get_local_window_utc(min_date)
        _, fetch_end = self._get_local_window_utc(max_date)

        # 2. Bulk Fetch Observations
        print(f"[{self.area.name}] Bulk fetching observations ({min_date} to {max_date})...")
        stmt_obs = select(WeatherModel).where(
            WeatherModel.station_code.in_(station_codes),
            WeatherModel.datetime_dt >= fetch_start,
            WeatherModel.datetime_dt < fetch_end
        ).order_by(WeatherModel.station_code, WeatherModel.datetime_dt)
        
        all_obs_rows = []
        for o in self.session.scalars(stmt_obs).all():
            all_obs_rows.append({
                "station_code": o.station_code,
                "datetime_dt": o.datetime_dt,
                "temp_f": o.temp_f,
                "dewpoint_f": o.dewpoint_f,
                "rel_humidity_pct": o.rel_humidity_pct,
                "wind_speed_mph": o.wind_speed_mph,
                "wind_gust_mph": o.wind_gust_mph,
                "wind_direction_t": o.wind_direction_t,
                "visibility_m": o.visibility_m,
                "pressure_inhg": o.pressure_inhg,
                "altimiter_setting_inhg": o.altimiter_setting_inhg,
                "onehr_precip_in": o.onehr_precip_in,
                "threehr_precip_in": o.threehr_precip_in,
                "sixhr_precip_in": o.sixhr_precip_in,
                "weather_t": o.weather_t,
                "clouds_t": o.clouds_t
            })
        
        df = pd.DataFrame(all_obs_rows)
        if df.empty: return

        # 3. Bulk Impute
        print(f"[{self.area.name}] Bulk imputing data gaps in memory...")
        df["datetime_dt"] = pd.to_datetime(df["datetime_dt"])
        episodic_cols = ["wind_gust_mph", "wind_direction_t", "weather_t", "clouds_t"]
        smooth_cols = ["temp_f", "dewpoint_f", "rel_humidity_pct", "wind_speed_mph", 
                       "pressure_inhg", "altimiter_setting_inhg"]
        
        df["visibility_m"] = df["visibility_m"].fillna(10.0)
        df["onehr_precip_in"] = df["onehr_precip_in"].fillna(0.0)
        df["threehr_precip_in"] = df["threehr_precip_in"].fillna(0.0)
        df["sixhr_precip_in"] = df["sixhr_precip_in"].fillna(0.0)

        clean_df_list = []
        for code, group in df.groupby("station_code"):
            group = group.sort_values("datetime_dt").set_index("datetime_dt")
            group[smooth_cols] = group[smooth_cols].interpolate(method='linear', limit_direction='both')
            group[episodic_cols] = group[episodic_cols].ffill().bfill()
            group["station_code"] = code
            clean_df_list.append(group.reset_index())
        
        df = pd.concat(clean_df_list)

        # 4. Group by Local Date
        print(f"[{self.area.name}] Grouping data for processing...")
        df["local_date"] = df["datetime_dt"].dt.tz_localize("UTC").dt.tz_convert(self.tz).dt.date
        obs_groups = df.groupby(["station_code", "local_date"])

        # 5. Filter and Yield
        stats = {"dst": 0, "mismatch": 0, "density": 0, "success": 0}
        min_obs = 100

        print(f"[{self.area.name}] Applying defensive filters to {len(summary_map)} candidate days...")

        for key, summary in summary_map.items():
            station_code, obs_date = key
            
            if self._is_dst_transition(obs_date):
                stats["dst"] += 1
                continue

            try:
                day_obs_df = obs_groups.get_group(key)
            except KeyError:
                stats["density"] += 1
                continue

            if len(day_obs_df) < min_obs:
                stats["density"] += 1
                continue

            obs_max = day_obs_df["temp_f"].max()
            obs_min = day_obs_df["temp_f"].min()
            max_f = float(summary.max_temp_f)
            min_f = float(summary.min_temp_f)

            if abs(obs_max - max_f) > consistency_threshold or abs(obs_min - min_f) > consistency_threshold:
                stats["mismatch"] += 1
                continue

            max_norm = float(summary.max_temp_normal or 65.0)
            daylight = summary.daylight_minutes
            proximity = float(summary.record_proximity or 0.75)
            day_feat = np.array([encode_daylight(daylight), proximity], dtype=np.float32)

            seq = np.zeros((MAX_SEQ_LEN, N_FEATURES), dtype=np.float32)
            day_obs_dicts = day_obs_df.to_dict('records')
            for i, row_dict in enumerate(day_obs_dicts[:MAX_SEQ_LEN]):
                seq[i] = self.encoder.encode(row_dict, max_norm, day_feat)

            stats["success"] += 1
            yield (
                seq,
                self._anomaly_to_target(max_f, max_norm),
                self._anomaly_to_target(min_f, float(summary.min_temp_normal or 50.0)),
                {
                    "station_code": station_code,
                    "date": obs_date.isoformat(),
                    "actual_max": max_f,
                    "actual_min": min_f,
                    "normal_max": max_norm
                }
            )

        print(f"[{self.area.name}] Training Set Summary:")
        print(f"  - Successfully processed: {stats['success']}")
        print(f"  - Skipped (DST Transition): {stats['dst']}")
        print(f"  - Skipped (Insufficient Density): {stats['density']}")
        print(f"  - Skipped (Label Mismatch > {consistency_threshold}F): {stats['mismatch']}")

    def make_arrays(self) -> Tuple[np.ndarray, np.ndarray, np.ndarray, List[dict]]:
        """Collect all training days into large NumPy arrays for Keras."""
        X, y_max, y_min, metas = [], [], [], []
        for sample in self.iter_training_days():
            X.append(sample[0])
            y_max.append(sample[1])
            y_min.append(sample[2])
            metas.append(sample[3])
            
        if not X:
            raise ValueError(f"No valid training data found for {self.area.area_key}")
            
        return np.stack(X), np.stack(y_max), np.stack(y_min), metas
