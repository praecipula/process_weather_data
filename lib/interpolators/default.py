import datetime
import functools
from sqlalchemy import select, inspect

class DefaultValueInterpolated:
    """
    Decorator that interpolates by using a default value for this field.

    A good example for this is "visibility" where no value = fully clear, so they just don't report it.

    """

    def __init__(self, param_name: str, default_value):
        self._param_name = param_name
        self._default_value = default_value

    def __call__(self, func):
        @functools.wraps(func)
        def interpolate(*args, **kwargs):
            decorated = args[0]
            dec_value = func(decorated) #Ironically, the only time we call the underlying function.
            if dec_value is not None:
                return dec_value
            else:
                return self._default_value
        return interpolate