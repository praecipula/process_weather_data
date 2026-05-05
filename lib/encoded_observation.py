"""
encoded_observation.v3.py
=========================
SQLAlchemy model for storing pre-computed encoded observation vectors,
and the factory function that produces them from WeatherModel objects.

Design
------
Rather than re-encoding raw WeatherModel rows every training run (which
requires re-running all interpolators and encoders), we persist the
encoded float vector to a separate table. Training then reads from this
table directly — fast, no interpolation overhead.

The encoded vector is stored as a JSON blob in a single TEXT column.
This keeps the schema stable even if N_FEATURES changes during development:
old rows are simply invalid and need to be regenerated, which is detected
via the schema_version column.

The factory function `encode_observation` is the seam between the two
models. It takes a WeatherModel (with session attached) and a SummaryFcstModel
(for day-level context), runs all the encoders, and returns a new
EncodedObservationModel ready to be added to the session.

Changes from previous version:
  - Base imported from lib.db (no longer defined here)
  - Removed local DeclarativeBase and TypeDecorator imports

Usage
-----
    # Produce and persist encoded observations for a set of raw rows:
    from lib.encoded_observation import encode_observation

    for obs in raw_observations:
        summary = get_summary(obs.station_code, obs.datetime_dt.date())
        encoded = encode_observation(obs, summary, area)
        session.add(encoded)
    session.commit()

    # Later, load directly for training:
    from lib.encoded_observation import EncodedObservationModel
    rows = session.scalars(select(EncodedObservationModel)
                           .where(EncodedObservationModel.station_code == "KSFO"))
    vectors = [row.as_array() for row in rows]
"""

import json
import datetime
import numpy as np
from typing import Optional
from sqlalchemy import String, Text, Integer, Index
from sqlalchemy.orm import Mapped, mapped_column

from lib.db import Base
from lib.weather_model import WeatherModel, SummaryFcstModel
from lib.area_config import AreaConfig
from lib.input_vector import (
    N_FEATURES,
    encode_temp_anomaly,
    encode_normalized,
    encode_humidity,
    encode_precip,
    encode_wind_direction,
    encode_datetime,
    encode_weather_tokens,
    encode_clouds,
    encode_station_geo,
    encode_daylight,
)


# Increment this any time N_FEATURES or the encoding logic changes.
# Rows in the DB with a different schema_version are stale and must
# be regenerated before they can be used for training.
CURRENT_SCHEMA_VERSION = 1


# =============================================================================
# ENCODED OBSERVATION MODEL
# =============================================================================

class EncodedObservationModel(Base):
    """
    Persisted encoded input vector for one WeatherModel observation.

    Each row corresponds 1:1 with a row in the weather table (via
    weather_id) and stores the fully encoded, normalized float vector
    that feeds into the LSTM.

    Columns
    -------
    id                  surrogate primary key
    weather_id          references weather.id (the source raw observation)
    station_code        denormalized for easy filtering without a join
    datetime_dt         denormalized for easy sequencing without a join
    area_key            which AreaConfig was used to encode (e.g. "sfbay")
    schema_version      encoding version; rows with old version are stale
    clim_normal_f       the climatological normal used for anomaly encoding
    vector_json         the 69-float encoded vector as a JSON array
    """
    __tablename__ = "encoded_observation"

    id: Mapped[int] = mapped_column(primary_key=True)
    weather_id: Mapped[int] = mapped_column(Integer, index=True)
    station_code: Mapped[str] = mapped_column(String(30), index=True)
    datetime_dt: Mapped[str] = mapped_column(Text, index=True)   # ISO8601
    area_key: Mapped[str] = mapped_column(String(20))
    schema_version: Mapped[int] = mapped_column(Integer, default=CURRENT_SCHEMA_VERSION)
    clim_normal_f: Mapped[Optional[float]]
    vector_json: Mapped[str] = mapped_column(Text)

    def __repr__(self) -> str:
        return (
            f"EncodedObservationModel("
            f"id={self.id!r}, "
            f"station={self.station_code!r}, "
            f"datetime={self.datetime_dt!r}, "
            f"area={self.area_key!r}, "
            f"schema_v={self.schema_version!r})"
        )

    def as_array(self) -> np.ndarray:
        """Deserialize vector_json back to a numpy float32 array."""
        return np.array(json.loads(self.vector_json), dtype=np.float32)

    @property
    def is_current_schema(self) -> bool:
        """True if this row was encoded with the current schema version."""
        return self.schema_version == CURRENT_SCHEMA_VERSION

    @property
    def parsed_datetime(self) -> datetime.datetime:
        """Return datetime_dt as a Python datetime object."""
        return datetime.datetime.fromisoformat(self.datetime_dt)


# =============================================================================
# FACTORY FUNCTION
# =============================================================================

def encode_observation(
    obs: WeatherModel,
    summary: Optional[SummaryFcstModel],
    area: AreaConfig,
) -> EncodedObservationModel:
    """
    Encode a WeatherModel observation into an EncodedObservationModel.

    This is the seam between the raw data model and the ML pipeline.
    Calls all interp_* methods on obs (which use the attached SQLAlchemy
    session to impute missing values), applies the encoding functions
    from input_vector.v3, and wraps the result in an ORM object ready
    to be persisted.

    Args:
        obs:     WeatherModel instance. Must have a SQLAlchemy session
                 attached (i.e. it was loaded via a session query, or
                 was added to a session before calling this function).
                 The session is used internally by the interpolators.
        summary: SummaryFcstModel for the same station and date.
                 Used for clim_normal_f, daylight_minutes, record_proximity.
                 May be None; safe defaults will be used.
        area:    AreaConfig for the prediction area. Used for station
                 index mapping, geo features, and temp normalization std.

    Returns:
        EncodedObservationModel with vector_json populated.
        Not yet added to any session — caller is responsible for session.add().

    Raises:
        AssertionError if the produced vector has wrong length.
    """
    # --- Day-level context from summary ---
    clim_normal_f = float(
        summary.max_temp_normal
        if summary and summary.max_temp_normal is not None
        else 65.0  # Bay Area fallback
    )

    daylight_norm = encode_daylight(
        summary.daylight_minutes if summary else None
    )
    record_proximity = float(
        np.clip(summary.record_proximity or 0.75, 0.0, 1.0)
        if summary else 0.75
    )
    day_features = np.array([daylight_norm, record_proximity], dtype=np.float32)

    # --- Station context from area config ---
    station = area.get_station(obs.station_code)
    station_idx = area.station_index_map.get(obs.station_code, 0)

    # --- Build feature vector ---
    features = []

    # 0-1: temperature anomaly
    features.extend(encode_temp_anomaly(
        obs.interp_temp_f(), clim_normal_f, area.temp_clim_std_f))

    # 2-3: dewpoint
    features.extend(encode_normalized(obs.interp_dewpoint_f(), -20.0, 80.0))

    # 4-5: relative humidity
    features.extend(encode_humidity(obs.interp_rel_humidity_pct()))

    # 6-7: wind speed
    features.extend(encode_normalized(obs.interp_wind_speed_mph(), 0.0, 60.0))

    # 8-9: wind gust
    features.extend(encode_normalized(obs.interp_wind_gust_mph(), 0.0, 80.0))

    # 10-11: visibility (missing = clear = 1.0)
    features.extend(encode_normalized(
        obs.interp_visibility_m(), 0.0, 10.0, missing_val=1.0))

    # 12-13: pressure (composite inhg / altimiter)
    features.extend(encode_normalized(obs.interp_pressure(), 28.0, 31.0))

    # 14-15: 1hr precip
    features.extend(encode_precip(obs.interp_onehr_precip_in()))

    # 16-17: 3hr precip
    features.extend(encode_precip(obs.interp_threehr_precip_in()))

    # 18-19: 6hr precip
    features.extend(encode_precip(obs.interp_sixhr_precip_in()))

    # 20-21: wind direction sin/cos
    features.extend(encode_wind_direction(obs.interp_wind_direction_t()))

    # 22-26: datetime (sin_hour, cos_hour, sin_doy, cos_doy, fraction_elapsed)
    features.extend(encode_datetime(obs.datetime_dt))

    # 27: station index (raw int for Embedding layer)
    features.append(float(station_idx))

    # 28-30: station geographic features
    if station is not None:
        features.extend(encode_station_geo(station).tolist())
    else:
        features.extend([0.0, 0.0, 0.0])

    # 31-53: weather multi-hot (23)
    features.extend(encode_weather_tokens(obs.interp_weather_t()).tolist())

    # 54-60: cloud coverage multi-hot (7)
    # 61-66: cloud altitude multi-hot (6)
    cov, alt_vec = encode_clouds(obs.interp_clouds_t())
    features.extend(cov.tolist())
    features.extend(alt_vec.tolist())

    # 67: daylight_minutes_norm
    features.append(float(day_features[0]))

    # 68: record_proximity
    features.append(float(day_features[1]))

    assert len(features) == N_FEATURES, \
        f"Expected {N_FEATURES} features, got {len(features)}"

    return EncodedObservationModel(
        weather_id=obs.id,
        station_code=obs.station_code,
        datetime_dt=obs.datetime_dt.isoformat(),
        area_key=area.area_key,
        schema_version=CURRENT_SCHEMA_VERSION,
        clim_normal_f=clim_normal_f,
        vector_json=json.dumps([float(f) for f in features]),
    )
