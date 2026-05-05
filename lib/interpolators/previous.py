import functools
from sqlalchemy import select, inspect

class PreviousValueInterpolated:
    """
    Decorator that carries forward the last known value for this field.
    Suitable for categorical or observational fields that don't change smoothly.
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

            # Look for the most recent non-null value for this station
            stmt = select(model).where(
                model.station_code == decorated.station_code,
                model.datetime_dt < decorated.datetime_dt,
                column_name != None
            ).order_by(model.datetime_dt.desc()).limit(1)
            
            previous_row = session.scalars(stmt).first()
            if previous_row:
                return getattr(previous_row, self._param_name)
            return None
        return interpolate
