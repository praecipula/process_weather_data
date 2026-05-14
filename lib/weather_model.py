"""
weather_model.py
===================
SQLAlchemy models for weather and summary_fcst tables, extended with
a full set of interpolated accessors for all ML-relevant fields.
"""

import datetime
import json
from typing import Optional
from sqlalchemy import String, Text, Integer, Float, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import select, inspect

from lib.db import Base, ISO8601DateTime
from lib.interpolators.linear import LinearInterpolated
from lib.interpolators.previous import PreviousValueInterpolated
from lib.interpolators.default import DefaultValueInterpolated


# =============================================================================
# WEATHER TABLE
# =============================================================================

class WeatherModel(Base):
    """
    SQLAlchemy model for the weather table.
    Raw observations from METAR and personal weather stations.
    """
    __tablename__ = "weather"
    __table_args__ = (
        UniqueConstraint('station_code', 'datetime_dt', name='idx_stn_time'),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    station_code: Mapped[str] = mapped_column(String(30))
    datetime_dt: Mapped[datetime.datetime] = mapped_column(ISO8601DateTime)
    temp_f: Mapped[Optional[float]]
    dewpoint_f: Mapped[Optional[float]]
    rel_humidity_pct: Mapped[Optional[int]]
    heat_index_f: Mapped[Optional[int]]
    wind_chill_f: Mapped[Optional[int]]
    wind_direction_t: Mapped[Optional[str]]
    wind_speed_mph: Mapped[Optional[int]]
    wind_gust_mph: Mapped[Optional[int]]
    visibility_m: Mapped[Optional[float]]
    weather_t: Mapped[Optional[str]]
    clouds_t: Mapped[Optional[str]]
    pressure_sea_mb: Mapped[Optional[float]]
    pressure_inhg: Mapped[Optional[float]]
    altimiter_setting_inhg: Mapped[Optional[float]]
    accumulated_precip_in: Mapped[Optional[float]]
    onehr_precip_in: Mapped[Optional[float]]
    threehr_precip_in: Mapped[Optional[float]]
    sixhr_precip_in: Mapped[Optional[float]]
    twentyfourhr_precip_in: Mapped[Optional[float]]
    sixhr_max_f: Mapped[Optional[int]]
    sixhr_min_f: Mapped[Optional[int]]
    twentyfourhr_max_f: Mapped[Optional[int]]
    twentyfourhr_min_f: Mapped[Optional[int]]

    def __repr__(self) -> str:
        return (
            f"WeatherModel(id={self.id!r}, station_code={self.station_code!r}, "
            f"datetime={self.datetime_dt!r}, temp_f={self.temp_f!r})"
        )

    # --- Interpolators ---

    @LinearInterpolated("heat_index_f")
    def interp_heat_index_f(self):
        return self.heat_index_f

    @LinearInterpolated("pressure_inhg")
    def interp_pressure_inhg(self):
        return self.pressure_inhg

    @PreviousValueInterpolated("twentyfourhr_max_f")
    def interp_twentyfourhr_max_f(self):
        return self.twentyfourhr_max_f

    @DefaultValueInterpolated("visibility_m", default_value=10.0)
    def interp_visibility_m(self):
        return self.visibility_m

    @LinearInterpolated("temp_f")
    def interp_temp_f(self):
        return self.temp_f

    @LinearInterpolated("dewpoint_f")
    def interp_dewpoint_f(self):
        return self.dewpoint_f

    @LinearInterpolated("rel_humidity_pct")
    def interp_rel_humidity_pct(self):
        return self.rel_humidity_pct

    @LinearInterpolated("wind_speed_mph")
    def interp_wind_speed_mph(self):
        return self.wind_speed_mph

    @PreviousValueInterpolated("wind_gust_mph")
    def interp_wind_gust_mph(self):
        return self.wind_gust_mph

    @PreviousValueInterpolated("wind_direction_t")
    def interp_wind_direction_t(self):
        return self.wind_direction_t

    @PreviousValueInterpolated("weather_t")
    def interp_weather_t(self):
        return self.weather_t

    @PreviousValueInterpolated("clouds_t")
    def interp_clouds_t(self):
        return self.clouds_t

    @LinearInterpolated("altimiter_setting_inhg")
    def interp_altimiter_setting_inhg(self):
        return self.altimiter_setting_inhg

    @DefaultValueInterpolated("onehr_precip_in", default_value=0.0)
    def interp_onehr_precip_in(self):
        return self.onehr_precip_in

    @DefaultValueInterpolated("threehr_precip_in", default_value=0.0)
    def interp_threehr_precip_in(self):
        return self.threehr_precip_in

    @DefaultValueInterpolated("sixhr_precip_in", default_value=0.0)
    def interp_sixhr_precip_in(self):
        return self.sixhr_precip_in

    def interp_pressure(self) -> Optional[float]:
        p = self.interp_pressure_inhg()
        if p is not None:
            return p
        return self.interp_altimiter_setting_inhg()


# =============================================================================
# CLIMATOLOGY TABLES
# =============================================================================

class ClimatologyHourly(Base):
    __tablename__ = "climatology_hourly"

    id: Mapped[int] = mapped_column(primary_key=True)
    station_code: Mapped[str] = mapped_column(String(30), index=True)
    month: Mapped[int] = mapped_column(Integer, index=True)
    day: Mapped[int] = mapped_column(Integer, index=True)
    hour: Mapped[int] = mapped_column(Integer, index=True)
    
    temp_normal_f: Mapped[Optional[float]]
    dewpoint_normal_f: Mapped[Optional[float]]
    wind_speed_normal_mph: Mapped[Optional[float]]
    precip_prob_pct: Mapped[Optional[float]]

    def __repr__(self) -> str:
        return (
            f"ClimatologyHourly(stn={self.station_code!r}, "
            f"date={self.month:02d}-{self.day:02d} {self.hour:02d}h)"
        )

class SummaryFcstModel(Base):
    __tablename__ = "summary_fcst"
    __table_args__ = (
        UniqueConstraint('station_code', 'date_d', name='idx_stn_date'),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    station_code: Mapped[str] = mapped_column(String(30))
    date_d: Mapped[datetime.datetime] = mapped_column(ISO8601DateTime)
    max_temp_f: Mapped[Optional[int]]
    max_temp_t: Mapped[Optional[str]]
    max_temp_record: Mapped[Optional[int]]
    max_temp_normal: Mapped[Optional[int]]
    min_temp_f: Mapped[Optional[int]]
    min_temp_t: Mapped[Optional[str]]
    min_temp_record: Mapped[Optional[int]]
    min_temp_normal: Mapped[Optional[int]]
    precip_in: Mapped[Optional[float]]
    precip_mtd_in: Mapped[Optional[float]]
    precip_mtd_normal_in: Mapped[Optional[float]]
    average_wind_mph: Mapped[Optional[float]]
    gust_wind_mph: Mapped[Optional[float]]
    sky_cover_pct: Mapped[Optional[float]]
    weather_cond_arr: Mapped[Optional[str]]
    sunrise_t: Mapped[Optional[str]]
    sunset_t: Mapped[Optional[str]]

    def __repr__(self) -> str:
        return (
            f"SummaryFcstModel(station={self.station_code!r}, "
            f"date={self.date_d!r}, max={self.max_temp_f!r}, min={self.min_temp_f!r})"
        )

    @property
    def has_actuals(self) -> bool:
        return self.max_temp_f is not None and self.min_temp_f is not None

    @property
    def daylight_minutes(self) -> Optional[float]:
        if not self.sunrise_t or not self.sunset_t:
            return None
        try:
            ref = self.date_d or datetime.datetime.now()
            sunrise = _parse_time_str(self.sunrise_t, ref)
            sunset = _parse_time_str(self.sunset_t, ref)
            return (sunset - sunrise).total_seconds() / 60.0
        except (ValueError, AttributeError):
            return None

    @property
    def record_proximity(self) -> Optional[float]:
        if not self.max_temp_normal or not self.max_temp_record:
            return None
        record = float(self.max_temp_record)
        if record == 0:
            return None
        return float(self.max_temp_normal) / record

    @property
    def weather_conditions(self) -> list:
        if not self.weather_cond_arr:
            return []
        try:
            return json.loads(self.weather_cond_arr)
        except (json.JSONDecodeError, TypeError):
            return []


def _parse_time_str(time_str: str,
                    ref_date: datetime.datetime) -> datetime.datetime:
    t = datetime.datetime.strptime(time_str.strip(), "%I:%M %p")
    return ref_date.replace(hour=t.hour, minute=t.minute,
                            second=0, microsecond=0)
