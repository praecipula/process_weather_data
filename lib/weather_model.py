import datetime
import functools
from typing import List
from typing import Optional
from sqlalchemy import ForeignKey
from sqlalchemy import String, Text, Integer, Float
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.types import TypeDecorator
from sqlalchemy import select, inspect


class Base(DeclarativeBase):
    pass

class ISO8601DateTime(TypeDecorator):
    impl = Text  # stored as TEXT in SQLite
    cache_ok = True

    def process_result_value(self, value, dialect):
        """DB -> Python: called when reading from the database."""
        if value is None:
            return None
        return datetime.datetime.fromisoformat(value)

    def process_bind_param(self, value, dialect):
        """Python -> DB: called when writing to the database."""
        if value is None:
            return None
        return value.isoformat()

class LinearInterpolated:
    """
    Decorator that interpolates linearly between two values of this underlying field.

    This decorator is applicable to any smooth, continuous, differentiable function - basically any physical pattern that happens in weather (approximately, chaos theory be damned). This is *not* applicable for some fields that report e.g. the wind direction (which is presented as a classifier field and we need to model this differently) or summary fields like "24hr max temp" which aren't smooth entries, but rather are a summary report.

    This, especially with extrapolation from previous values, implies that there is a hidden "true" value not being reported by the weather station for that point in time. Filling in the value is then intended to represent this hidden model, and if we could have perfect knowledge, the interpolation and extrapolation would converge.

    * If there is a value for this field, the result is that value.
    * If there is no "previous" entry for this same station / time that has a value for this field, extrapolate backwards from next 2
    * If there is no "next" entry for this same station / time that has a value for this field, extrapolate forwards from the last 2
    * If there is both, interpolate between these.
    * If there is neither, return None. Note that this means it *is* possible to have a None return value from this interpolation.
    """

    def __init__(self, param_name: str):
        self._param_name = param_name

    def __call__(self, func):
        @functools.wraps(func)
        def interpolate(*args, **kwargs):
            decorated = args[0]
            dec_value = func(decorated) #Ironically, the only time we call the underlying function.
            if dec_value is not None:
                return dec_value
            # Noteworthy: we use the session *from the decorated object* as this session will (at runtime) be a real session, but during testing it's a synthetic / test harness one. This way we can inspect it instead of having to e.g. pass a factory around everywhere.
            session = inspect(decorated).session
            column_name = getattr(WeatherModel, self._param_name)

            def get_previous_row(row, column_name):
                previous_row_statement = select(WeatherModel).where(
                    WeatherModel.station_code == row.station_code,
                    WeatherModel.datetime_dt < row.datetime_dt,
                    column_name != None
                ).order_by(WeatherModel.datetime_dt.desc()).limit(1)
                return session.scalars(previous_row_statement).first()

            def get_next_row(row, column_name):
                next_row_statement = select(WeatherModel).where(
                    WeatherModel.station_code == row.station_code,
                    WeatherModel.datetime_dt > row.datetime_dt,
                    column_name != None
                ).order_by(WeatherModel.datetime_dt.asc()).limit(1)
                return session.scalars(next_row_statement).first()

            previous_row = get_previous_row(decorated, column_name)
            previous_value = None if previous_row is None else getattr(previous_row, self._param_name)

            next_row = get_next_row(decorated, column_name)
            next_value = None if next_row is None else getattr(next_row, self._param_name)

            # Central interpolation (middle value missing) - the most usual case for interpolation.
            if previous_value is not None and next_value is not None:
                interp = (next_value - previous_value) / (next_row.datetime_dt - previous_row.datetime_dt).seconds * (decorated.datetime_dt - previous_row.datetime_dt).seconds + previous_value
                return interp
            elif previous_value is not None and next_value is None:
                # Extrapolate forwards.
                second_previous_row = get_previous_row(previous_row, column_name)
                second_previous_value = None if second_previous_row is None else getattr(second_previous_row, self._param_name)
                if second_previous_value is None:
                    # Need at least 2 rows to extrapolate from
                    return None
                extrap = (previous_value - second_previous_value) / (previous_row.datetime_dt - second_previous_row.datetime_dt).seconds * (decorated.datetime_dt - previous_row.datetime_dt).seconds + previous_value
                return extrap
            elif previous_value is None and next_value is not None:
                # Extrapolate backwards.
                second_next_row = get_next_row(next_row, column_name)
                second_next_value = None if second_next_row is None else getattr(second_next_row, self._param_name)
                if second_next_value is None:
                    # Need at least 2 rows to extrapolate from
                    return None
                extrap = next_value - ((second_next_value - next_value) / (second_next_row.datetime_dt - next_row.datetime_dt).seconds * (next_row.datetime_dt - decorated.datetime_dt).seconds)
                return extrap
        return interpolate

class PreviousValueInterpolated:
    """
    Decorator that interpolates by using the last known value for this field.

    This decorator is applicable to any value where the report stops simply repeating a previous value (e.g. "24hr max" which isn't represented in 5-minutely-snapshots, but only at regular times) and for non-reported classifier fields (e.g. "weather" status, or even wind direction).

    This can be considered a degenerate case of nearest neighbor interpolation, but it's important to note that we have a stronger case for "this stayed the same as last time" vs. interpolating backwards from a future value.

    Remember, ultimately the network is intended to be trained to predict the most up-to-date value for a given point in time, so we can't interpolate backwards for new evaluations. For discontinuous classifier fields, then, we don't have a hidden value that we're estimating, things could vary wildly in the middle and so this is meant more for "we only know when we measure it" type fields.

    * If there is a value for this field, the result is that value.
    * If there is a previous entry for this same station / time that has a value for this field, return that value.
    * If there is no "previous" entry for this same station / time that has a value for this field, it is null - there is no information whatsoever.

    """

    def __init__(self, param_name: str):
        self._param_name = param_name

    def __call__(self, func):
        @functools.wraps(func)
        def interpolate(*args, **kwargs):
            decorated = args[0]
            dec_value = func(decorated) #Ironically, the only time we call the underlying function.
            if dec_value is not None:
                return dec_value
            # Noteworthy: we use the session *from the decorated object* as this session will (at runtime) be a real session, but during testing it's a synthetic / test harness one. This way we can inspect it instead of having to e.g. pass a factory around everywhere.
            session = inspect(decorated).session
            column_name = getattr(WeatherModel, self._param_name)

            def get_previous_row(row, column_name):
                previous_row_statement = select(WeatherModel).where(
                    WeatherModel.station_code == row.station_code,
                    WeatherModel.datetime_dt < row.datetime_dt,
                    column_name != None
                ).order_by(WeatherModel.datetime_dt.desc()).limit(1)
                return session.scalars(previous_row_statement).first()

            previous_row = get_previous_row(decorated, column_name)
            previous_value = None if previous_row is None else getattr(previous_row, self._param_name)
            return previous_value
        return interpolate


class WeatherModel(Base):
    """
    A SqlAlchemy model for working with our database.
    For now, we're using our legacy database - the definition of which is created with raw SQL in `compile_to_sqlite.py`. Eventually it might be worth migrating this to have the entire thing mapped to sqlalchemy, but I wanted to start gathering the data and putting it in *some* db asap in order to have data to migrate from...
    """
    __tablename__ = "weather"

    id: Mapped[int] = mapped_column(primary_key=True)
    station_code: Mapped[str] = mapped_column(String(30))
    datetime_dt: Mapped[datetime] = mapped_column(ISO8601DateTime)
    temp_f: Mapped[float | None]
    dewpoint_f: Mapped[float | None]
    rel_humidity_pct: Mapped[int | None]
    heat_index_f: Mapped[int | None]
    wind_chill_f: Mapped[int | None]
    wind_direction_t: Mapped[str | None]
    wind_speed_mph: Mapped[int | None]
    wind_gust_mph: Mapped[int | None]
    visibility_m: Mapped[float | None]
    weather_t: Mapped[str | None]
    clouds_t: Mapped[str | None]
    pressure_sea_mb: Mapped[float | None]
    pressure_inhg: Mapped[float | None]
    altimiter_setting_inhg: Mapped[float | None]
    accumulated_precip_in: Mapped[float | None]
    onehr_precip_in: Mapped[float | None]
    threehr_precip_in: Mapped[float | None]
    sixhr_precip_in: Mapped[float | None]
    twentyfourhr_precip_in: Mapped[float | None]
    sixhr_max_f: Mapped[int | None]
    sixhr_min_f: Mapped[int | None]
    twentyfourhr_max_f: Mapped[int | None]
    twentyfourhr_min_f: Mapped[int | None]

    def __repr__(self) -> str:
        return f"WeatherModel(id={self.id!r}, station_code={self.station_code!r}, datetime={self.datetime_dt!r}, temp_f={self.temp_f!r} ...)"

    @LinearInterpolated("heat_index_f")
    def interp_heat_index_f(self):
        # By convention, the interpolation functions should return their underlying value for use in the interpoator. As this value for interpolatable fields might be null, we trust that the interpolator will do its best to make that null a not-null value.
        return self.heat_index_f
    
    @LinearInterpolated("pressure_inhg")
    def interp_pressure_inhg(self):
        return self.pressure_inhg

    @PreviousValueInterpolated("twentyfourhr_max_f")
    def interp_twentyfourhr_max_f(self):
        return self.twentyfourhr_max_f  