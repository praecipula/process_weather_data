import datetime
import functools
from sqlalchemy import select, inspect

class NumericalVocabularyInterpolated:
    """
    Decorator that assigns a unique index to each item in the vocabulary of values for this field.

    There are some nuances in tokenization, but for now let's just apply some rules for these and see if we need to refine them later.
    """

    def __init__(self, param_name: str):
        self._param_name = param_name
        self._vocab = []


    def __call__(self, func):
        @functools.wraps(func)
        def interpolate(*args, **kwargs):
            decorated = args[0]
            dec_value = func(decorated) #Ironically, the only time we call the underlying function.
            # Now we need to standardize the value.

        return interpolate

"""
Sample distinct weather_t values:

fog

mist,fog
mist
haze
light rain
Mist
Haze
Lt rain
Fog
Lt rain, Mist
Rain
Hvy rain
Rain, Mist
Hvy rain, Mist
Lt thunder shwr
light rain,mist
light rain/thunderstorm,mist
rain
thunderstorm
light rain/thunderstorm
thunderstorm,mist
rain,mist
fog,mist
heavy rain,mist
thunder
light drizzle
thunder,mist
heavy rain/thunderstorm,mist
light drizzle,mist

Sample distinct cloud_t values:


"""