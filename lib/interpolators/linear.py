import datetime
import functools
from sqlalchemy import select, inspect
from typing import Dict, Any, Optional

# Global cache: {(station_code, datetime_dt): WeatherModel}
# In a real high-load system, this might be Redis or a bounded LRU cache.
# For our training script, a simple dict is fine.
_OBSERVATION_CACHE: Dict[tuple, Any] = {}

def get_cached_observation(session, model, station_code, datetime_dt):
    key = (station_code, datetime_dt)
    if key in _OBSERVATION_CACHE:
        return _OBSERVATION_CACHE[key]
    
    stmt = select(model).where(
        model.station_code == station_code,
        model.datetime_dt == datetime_dt
    )
    res = session.scalars(stmt).first()
    if res:
        _OBSERVATION_CACHE[key] = res
    return res

class LinearInterpolated:
    """
    Refactored LinearInterpolated with in-memory caching support.
    """

    def __init__(self, param_name: str):
        self._param_name = param_name

    def __call__(self, func):
        @functools.wraps(func)
        def interpolate(*args, **kwargs):
            decorated = args[0]
            dec_value = func(decorated)
            if dec_value is not None:
                return dec_value
            
            session = inspect(decorated).session
            model = type(decorated)
            column_name = getattr(model, self._param_name)

            def get_previous_row(row, column_name):
                previous_row_statement = select(model).where(
                    model.station_code == row.station_code,
                    model.datetime_dt < row.datetime_dt,
                    column_name != None
                ).order_by(model.datetime_dt.desc()).limit(1)
                return session.scalars(previous_row_statement).first()

            def get_next_row(row, column_name):
                next_row_statement = select(model).where(
                    model.station_code == row.station_code,
                    model.datetime_dt > row.datetime_dt,
                    column_name != None
                ).order_by(model.datetime_dt.asc()).limit(1)
                return session.scalars(next_row_statement).first()

            previous_row = get_previous_row(decorated, column_name)
            previous_value = None if previous_row is None else getattr(previous_row, self._param_name)

            next_row = get_next_row(decorated, column_name)
            next_value = None if next_row is None else getattr(next_row, self._param_name)

            if previous_value is not None and next_value is not None:
                interp = (next_value - previous_value) / (next_row.datetime_dt - previous_row.datetime_dt).total_seconds() * (decorated.datetime_dt - previous_row.datetime_dt).total_seconds() + previous_value
                return interp
            elif previous_value is not None and next_value is None:
                second_previous_row = get_previous_row(previous_row, column_name)
                second_previous_value = None if second_previous_row is None else getattr(second_previous_row, self._param_name)
                if second_previous_value is None:
                    return None
                extrap = (previous_value - second_previous_value) / (previous_row.datetime_dt - second_previous_row.datetime_dt).total_seconds() * (decorated.datetime_dt - previous_row.datetime_dt).total_seconds() + previous_value
                return extrap
            elif previous_value is None and next_value is not None:
                second_next_row = get_next_row(next_row, column_name)
                second_next_value = None if second_next_row is None else getattr(second_next_row, self._param_name)
                if second_next_value is None:
                    return None
                extrap = next_value - ((second_next_value - next_value) / (second_next_row.datetime_dt - next_row.datetime_dt).total_seconds() * (next_row.datetime_dt - decorated.datetime_dt).total_seconds())
                return extrap
        return interpolate
