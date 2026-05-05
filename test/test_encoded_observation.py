"""
test_encoded_observation.py
===========================
Tests for EncodedObservationModel and the encode_observation() factory.

Test strategy
-------------
All tests use in-memory SQLite sessions following the pattern established
in test_weather_model_fetch.py. No real database is touched.

Because all models now share a single Base (from lib.db), one call to
Base.metadata.create_all() creates all tables — weather, summary_fcst,
and encoded_observation — which is fine for in-memory test DBs.

We test:
  1. encode_observation() produces an EncodedObservationModel with the
     right shape, type, and metadata fields.
  2. as_array() round-trips through JSON correctly.
  3. Specific feature values are encoded as expected (spot-checks on
     well-understood features like wind direction, cloud encoding).
  4. Missing fields are handled gracefully (no crash, sensible defaults).
  5. summary=None is handled gracefully (no crash, fallback values used).
  6. schema_version is set correctly and is_current_schema works.
  7. Stale schema detection (rows with old version flagged correctly).
"""

import json
import math
import pytest
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, insert, select
from sqlalchemy.orm import Session

# Single Base import — creates all tables (weather, summary_fcst,
# encoded_observation) in one create_all() call.
from lib.db import Base
from lib.weather_model import WeatherModel, SummaryFcstModel
from lib.encoded_observation import (
    EncodedObservationModel,
    encode_observation,
    CURRENT_SCHEMA_VERSION,
)
from lib.area_config import SAN_FRANCISCO
from lib.input_vector import N_FEATURES


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def engine():
    """
    In-memory SQLite engine with all tables created from the shared Base.
    Creates weather, summary_fcst, and encoded_observation tables.
    """
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    return eng


@pytest.fixture
def minimal_obs(engine):
    """
    A single WeatherModel row with all fields populated.
    Represents a clean KSFO observation with no missing values.
    """
    with Session(engine) as session:
        session.execute(insert(WeatherModel).values(
            id=1,
            station_code="KSFO",
            datetime_dt=datetime.fromisoformat("2026-03-12T09:00:00"),
            temp_f=58.0,
            dewpoint_f=46.0,
            rel_humidity_pct=72,
            wind_direction_t="WNW",
            wind_speed_mph=10,
            wind_gust_mph=None,
            visibility_m=10.0,
            weather_t=None,
            clouds_t="CLR",
            pressure_inhg=30.15,
            altimiter_setting_inhg=30.16,
            onehr_precip_in=None,
            threehr_precip_in=None,
            sixhr_precip_in=None,
        ))
        session.commit()

        obs = session.scalars(
            select(WeatherModel).where(WeatherModel.id == 1)
        ).first()
        yield obs, session


@pytest.fixture
def minimal_obs_with_summary(engine):
    """
    A WeatherModel row paired with a SummaryFcstModel for the same day.
    """
    with Session(engine) as session:
        session.execute(insert(WeatherModel).values(
            id=1,
            station_code="KSFO",
            datetime_dt=datetime.fromisoformat("2026-03-12T09:00:00"),
            temp_f=58.0,
            dewpoint_f=46.0,
            rel_humidity_pct=72,
            wind_direction_t="WNW",
            wind_speed_mph=10,
            wind_gust_mph=None,
            visibility_m=10.0,
            weather_t=None,
            clouds_t="CLR",
            pressure_inhg=30.15,
            altimiter_setting_inhg=30.16,
            onehr_precip_in=None,
            threehr_precip_in=None,
            sixhr_precip_in=None,
        ))
        session.execute(insert(SummaryFcstModel).values(
            id=1,
            station_code="KSFO",
            date_d="2026-03-12T00:00:00",
            max_temp_f=72,
            min_temp_f=50,
            max_temp_normal=63,
            max_temp_record=82,
            min_temp_normal=47,
            min_temp_record=33,
            sunrise_t="7:23 AM",
            sunset_t="7:15 PM",
        ))
        session.commit()

        obs = session.scalars(
            select(WeatherModel).where(WeatherModel.id == 1)
        ).first()
        summary = session.scalars(
            select(SummaryFcstModel).where(SummaryFcstModel.station_code == "KSFO")
        ).first()
        yield obs, summary, session


@pytest.fixture
def sparse_obs(engine):
    """
    A WeatherModel row with most fields missing.
    Represents a personal weather station with minimal reporting.
    Tests that missing value handling doesn't crash.
    """
    with Session(engine) as session:
        session.execute(insert(WeatherModel).values(
            id=2,
            station_code="KCASANBR16",
            datetime_dt=datetime.fromisoformat("2026-04-17T11:29:00"),
            temp_f=67.1,
            dewpoint_f=None,
            rel_humidity_pct=None,
            wind_direction_t="North",
            wind_speed_mph=None,
            wind_gust_mph=None,
            visibility_m=None,
            weather_t=None,
            clouds_t=None,
            pressure_inhg=None,
            altimiter_setting_inhg=30.08,
            onehr_precip_in=None,
            threehr_precip_in=None,
            sixhr_precip_in=None,
        ))
        session.commit()

        obs = session.scalars(
            select(WeatherModel).where(WeatherModel.id == 2)
        ).first()
        yield obs, session


# =============================================================================
# BASIC SHAPE AND TYPE TESTS
# =============================================================================

def test_encode_observation_returns_model(minimal_obs):
    obs, session = minimal_obs
    result = encode_observation(obs, summary=None, area=SAN_FRANCISCO)
    assert isinstance(result, EncodedObservationModel)


def test_encoded_vector_has_correct_length(minimal_obs):
    obs, session = minimal_obs
    result = encode_observation(obs, summary=None, area=SAN_FRANCISCO)
    vec = result.as_array()
    assert vec.shape == (N_FEATURES,), \
        f"Expected shape ({N_FEATURES},), got {vec.shape}"


def test_encoded_vector_is_float32(minimal_obs):
    obs, session = minimal_obs
    result = encode_observation(obs, summary=None, area=SAN_FRANCISCO)
    vec = result.as_array()
    assert vec.dtype == np.float32


def test_encoded_vector_has_no_nan(minimal_obs):
    obs, session = minimal_obs
    result = encode_observation(obs, summary=None, area=SAN_FRANCISCO)
    vec = result.as_array()
    assert not np.any(np.isnan(vec)), \
        f"NaN at indices: {np.where(np.isnan(vec))}"


def test_encoded_vector_has_no_inf(minimal_obs):
    obs, session = minimal_obs
    result = encode_observation(obs, summary=None, area=SAN_FRANCISCO)
    vec = result.as_array()
    assert not np.any(np.isinf(vec)), \
        f"Inf at indices: {np.where(np.isinf(vec))}"


# =============================================================================
# METADATA FIELD TESTS
# =============================================================================

def test_metadata_fields_are_populated(minimal_obs):
    obs, session = minimal_obs
    result = encode_observation(obs, summary=None, area=SAN_FRANCISCO)
    assert result.weather_id == 1
    assert result.station_code == "KSFO"
    assert result.area_key == "sfbay"
    assert result.schema_version == CURRENT_SCHEMA_VERSION
    assert result.datetime_dt == "2026-03-12T09:00:00"


def test_clim_normal_fallback_when_no_summary(minimal_obs):
    obs, session = minimal_obs
    result = encode_observation(obs, summary=None, area=SAN_FRANCISCO)
    assert result.clim_normal_f == 65.0


def test_clim_normal_from_summary(minimal_obs_with_summary):
    obs, summary, session = minimal_obs_with_summary
    result = encode_observation(obs, summary=summary, area=SAN_FRANCISCO)
    assert result.clim_normal_f == 63.0


# =============================================================================
# ROUND-TRIP PERSISTENCE TEST
# =============================================================================

def test_can_persist_and_reload(minimal_obs_with_summary, engine):
    obs, summary, session = minimal_obs_with_summary
    encoded = encode_observation(obs, summary=summary, area=SAN_FRANCISCO)

    session.add(encoded)
    session.commit()

    reloaded = session.scalars(
        select(EncodedObservationModel).where(
            EncodedObservationModel.weather_id == 1
        )
    ).first()

    assert reloaded is not None
    vec = reloaded.as_array()
    assert vec.shape == (N_FEATURES,)
    assert not np.any(np.isnan(vec))


def test_as_array_round_trips_values(minimal_obs_with_summary):
    obs, summary, session = minimal_obs_with_summary
    encoded = encode_observation(obs, summary=summary, area=SAN_FRANCISCO)

    original = encoded.as_array()
    reloaded = np.array(json.loads(encoded.vector_json), dtype=np.float32)
    np.testing.assert_array_equal(original, reloaded)


# =============================================================================
# FEATURE VALUE SPOT-CHECKS
# =============================================================================

def test_wind_direction_wnw_encodes_correctly(minimal_obs):
    """WNW = 292.5 degrees. sin(292.5°) ≈ -0.924, cos(292.5°) ≈ 0.383"""
    obs, session = minimal_obs
    result = encode_observation(obs, summary=None, area=SAN_FRANCISCO)
    vec = result.as_array()

    expected_sin = math.sin(2 * math.pi * 292.5 / 360)
    expected_cos = math.cos(2 * math.pi * 292.5 / 360)
    assert abs(vec[20] - expected_sin) < 1e-5, f"sin_wind: {vec[20]} != {expected_sin}"
    assert abs(vec[21] - expected_cos) < 1e-5, f"cos_wind: {vec[21]} != {expected_cos}"


def test_fraction_elapsed_at_9am(minimal_obs):
    """9:00 AM = 540 minutes = 540/1440 = 0.375"""
    obs, session = minimal_obs
    result = encode_observation(obs, summary=None, area=SAN_FRANCISCO)
    vec = result.as_array()
    assert abs(vec[26] - 540 / 1440.0) < 1e-5


def test_clear_sky_encodes_clr_coverage(minimal_obs):
    """CLR should set bit 0 of cloud coverage multi-hot (index 54)."""
    obs, session = minimal_obs
    result = encode_observation(obs, summary=None, area=SAN_FRANCISCO)
    vec = result.as_array()
    assert vec[54] == 1.0, f"CLR coverage bit should be 1.0, got {vec[54]}"
    assert all(vec[55:61] == 0.0), f"Other coverage bits should be 0: {vec[55:61]}"


def test_visibility_missing_defaults_to_max(sparse_obs):
    """Missing visibility -> imputed to 1.0 (clear) with missing_flag=1.0."""
    obs, session = sparse_obs
    result = encode_observation(obs, summary=None, area=SAN_FRANCISCO)
    vec = result.as_array()
    assert vec[10] == 1.0, f"visibility_norm should be 1.0, got {vec[10]}"
    assert vec[11] == 1.0, f"visibility missing_flag should be 1.0, got {vec[11]}"


def test_station_index_ksfo_is_zero(minimal_obs):
    """KSFO is primary station, index 0 in SAN_FRANCISCO area config."""
    obs, session = minimal_obs
    result = encode_observation(obs, summary=None, area=SAN_FRANCISCO)
    vec = result.as_array()
    assert vec[27] == 0.0, f"KSFO station index should be 0, got {vec[27]}"


def test_daylight_from_summary(minimal_obs_with_summary):
    """
    Sunrise 7:23 AM, Sunset 7:15 PM = 712 minutes.
    Normalized: (712 - 570) / (870 - 570) = 142/300 ≈ 0.473
    """
    obs, summary, session = minimal_obs_with_summary
    result = encode_observation(obs, summary=summary, area=SAN_FRANCISCO)
    vec = result.as_array()
    expected = (712 - 570) / (870 - 570)
    assert abs(vec[67] - expected) < 0.01, \
        f"daylight_norm: {vec[67]:.3f} != {expected:.3f}"


def test_record_proximity_from_summary(minimal_obs_with_summary):
    """max_temp_normal=63, max_temp_record=82 -> 63/82 ≈ 0.768"""
    obs, summary, session = minimal_obs_with_summary
    result = encode_observation(obs, summary=summary, area=SAN_FRANCISCO)
    vec = result.as_array()
    expected = 63.0 / 82.0
    assert abs(vec[68] - expected) < 1e-4, \
        f"record_proximity: {vec[68]:.4f} != {expected:.4f}"


# =============================================================================
# MISSING DATA HANDLING
# =============================================================================

def test_sparse_obs_does_not_crash(sparse_obs):
    """Personal weather station with most fields missing should not crash."""
    obs, session = sparse_obs
    result = encode_observation(obs, summary=None, area=SAN_FRANCISCO)
    assert result is not None
    vec = result.as_array()
    assert vec.shape == (N_FEATURES,)
    assert not np.any(np.isnan(vec))


def test_no_summary_uses_fallback_daylight(minimal_obs):
    """Without a summary, daylight_norm should be the midpoint fallback (0.5)."""
    obs, session = minimal_obs
    result = encode_observation(obs, summary=None, area=SAN_FRANCISCO)
    vec = result.as_array()
    assert vec[67] == 0.5, f"Fallback daylight_norm should be 0.5, got {vec[67]}"


def test_no_summary_uses_fallback_record_proximity(minimal_obs):
    """Without a summary, record_proximity should be the fallback (0.75)."""
    obs, session = minimal_obs
    result = encode_observation(obs, summary=None, area=SAN_FRANCISCO)
    vec = result.as_array()
    assert vec[68] == 0.75, f"Fallback record_proximity should be 0.75, got {vec[68]}"


# =============================================================================
# SCHEMA VERSION TESTS
# =============================================================================

def test_schema_version_is_current(minimal_obs):
    obs, session = minimal_obs
    result = encode_observation(obs, summary=None, area=SAN_FRANCISCO)
    assert result.schema_version == CURRENT_SCHEMA_VERSION
    assert result.is_current_schema is True


def test_stale_schema_detection(minimal_obs_with_summary, engine):
    """A row written with an old schema_version should be flagged as stale."""
    obs, summary, session = minimal_obs_with_summary
    encoded = encode_observation(obs, summary=summary, area=SAN_FRANCISCO)
    encoded.schema_version = CURRENT_SCHEMA_VERSION - 1

    session.add(encoded)
    session.commit()

    reloaded = session.scalars(
        select(EncodedObservationModel).where(
            EncodedObservationModel.weather_id == 1
        )
    ).first()

    assert reloaded.is_current_schema is False
