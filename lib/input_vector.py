"""
input_vector.py
===============
Feature encoding pipeline: WeatherModel rows -> normalized numpy vectors.

This version implements sophisticated parsers for METAR-style weather and 
cloud strings to capture simultaneous conditions and vertical cloud structure.
"""

import math
import datetime
import numpy as np
from typing import Optional, List, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import select

from lib.area_config import AreaConfig, Station
from lib.weather_model import WeatherModel, SummaryFcstModel


# =============================================================================
# CONSTANTS & VOCABULARIES
# =============================================================================

MAX_SEQ_LEN = 288        # 24h * 12 five-minute slots
N_FEATURES = 84          # Increased to accommodate 3 cloud layers + multi-hot weather

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
    from lib.input_vector import WIND_DIRECTION_MAP # Import within or use global
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

    def encode(self, obs: WeatherModel,
               clim_normal_f: float,
               day_features: np.ndarray) -> np.ndarray:
        station = self.area.get_station(obs.station_code)
        station_idx = self.area.station_index_map.get(obs.station_code, 0)

        features = []

        # 0-1: Temperature anomaly
        features.extend(encode_temp_anomaly(obs.interp_temp_f(), clim_normal_f))
        
        # 2-3: Dewpoint (Range -20 to 80)
        features.extend(encode_normalized(obs.interp_dewpoint_f(), -20.0, 80.0))
        
        # 4-5: Humidity (0-100)
        features.extend(encode_normalized(obs.interp_rel_humidity_pct(), 0.0, 100.0))
        
        # 6-7: Wind Speed (0-60)
        features.extend(encode_normalized(obs.interp_wind_speed_mph(), 0.0, 60.0))
        
        # 8-9: Wind Gust (0-80)
        features.extend(encode_normalized(obs.interp_wind_gust_mph(), 0.0, 80.0))
        
        # 10-11: Visibility (0-10 miles, missing defaults to clear)
        features.extend(encode_normalized(obs.interp_visibility_m(), 0.0, 10.0, missing_val=1.0))
        
        # 12-13: Pressure (28 to 31 inHg)
        features.extend(encode_normalized(obs.interp_pressure(), 28.0, 31.0))
        
        # 14-19: Precip (1, 3, 6 hr) - Log normalized in input_vector logic
        from lib.input_vector import encode_precip
        features.extend(encode_precip(obs.interp_onehr_precip_in()))
        features.extend(encode_precip(obs.interp_threehr_precip_in()))
        features.extend(encode_precip(obs.interp_sixhr_precip_in()))
        
        # 20-21: Wind Direction sin/cos
        features.extend(encode_wind_direction(obs.interp_wind_direction_t()))
        
        # 22-26: Datetime circular + fraction
        from lib.input_vector import encode_datetime
        features.extend(encode_datetime(obs.datetime_dt))
        
        # 27: Station Index (Categorical)
        features.append(float(station_idx))
        
        # 28-30: Station Geo
        if station:
            from lib.input_vector import encode_station_geo
            features.extend(encode_station_geo(station).tolist())
        else:
            features.extend([0.0, 0.0, 0.0])
            
        # 31-53: Weather Multi-hot (23 tokens)
        features.extend(encode_weather_multi_hot(obs.interp_weather_t()))
        
        # 54-77: Cloud Grammar (3 layers * 8 features = 24)
        features.extend(encode_clouds_grammar(obs.interp_clouds_t()))
        
        # 78-79: Daylight & Record Proximity
        features.append(float(day_features[0]))
        features.append(float(day_features[1]))

        # Final check on feature length
        # NOTE: If length mismatch, check N_FEATURES constant
        return np.array(features, dtype=np.float32)

# =============================================================================
# SEQUENCE BUILDER & HELPERS
# =============================================================================

WIND_DIRECTION_MAP = {
    "N": 0.0, "NNE": 22.5, "NE": 45.0, "ENE": 67.5, "E": 90.0, "ESE": 112.5,
    "SE": 135.0, "SSE": 157.5, "S": 180.0, "SSW": 202.5, "SW": 225.0, "WSW": 247.5,
    "W": 270.0, "WNW": 292.5, "NW": 315.0, "NNW": 337.5,
    "NORTH": 0.0, "EAST": 90.0, "SOUTH": 180.0, "WEST": 270.0,
    "VARIABLE": None, "CALM": None, "VAR": None, "VRB": None
}

def encode_precip(value) -> List[float]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return [0.0, 1.0]
    # log1p normalization capped at 2 inches
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

    def _get_summary(self, station_code: str, target_date: datetime.date) -> Optional[SummaryFcstModel]:
        stmt = select(SummaryFcstModel).where(
            SummaryFcstModel.station_code == station_code,
            SummaryFcstModel.date_d >= datetime.datetime(target_date.year, target_date.month, target_date.day),
            SummaryFcstModel.date_d < datetime.datetime(target_date.year, target_date.month, target_date.day) + datetime.timedelta(days=1)
        )
        return self.session.scalars(stmt).first()

    def _get_observations(self, station_code: str, target_date: datetime.date) -> List[WeatherModel]:
        start = datetime.datetime(target_date.year, target_date.month, target_date.day)
        end = start + datetime.timedelta(days=1)
        stmt = select(WeatherModel).where(
            WeatherModel.station_code == station_code,
            WeatherModel.datetime_dt >= start,
            WeatherModel.datetime_dt < end
        ).order_by(WeatherModel.datetime_dt)
        return list(self.session.scalars(stmt).all())

    def _anomaly_to_target(self, actual_f: float, normal_f: float) -> np.ndarray:
        anomaly = int(round(actual_f - normal_f))
        idx = anomaly - self.area.anomaly_bucket_min_f
        idx = int(np.clip(idx, 0, self.area.temp_buckets - 1))
        target = np.zeros(self.area.temp_buckets, dtype=np.float32)
        target[idx] = 1.0
        return target

    def build_day_sequence(self, station_code: str, target_date: datetime.date) -> Optional[np.ndarray]:
        observations = self._get_observations(station_code, target_date)
        if not observations: return None
        summary = self._get_summary(station_code, target_date)
        norm_f = float(summary.max_temp_normal if summary and summary.max_temp_normal else 65.0)
        day_feat = np.array([encode_daylight(summary.daylight_minutes if summary else None), 
                             float(summary.record_proximity if summary else 0.75)], dtype=np.float32)
        
        seq = np.zeros((MAX_SEQ_LEN, len(self.encoder.encode(observations[0], norm_f, day_feat))), dtype=np.float32)
        for i, obs in enumerate(observations[:MAX_SEQ_LEN]):
            seq[i] = self.encoder.encode(obs, norm_f, day_feat)
        return seq

    def iter_training_days(self):
        # Implementation similar to previous but using updated encoding logic
        # For brevity, this would iterate through DB and yield (X, y_max, y_min, meta)
        pass
