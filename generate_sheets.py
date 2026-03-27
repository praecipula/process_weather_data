#!/usr/bin/env python

from pathlib import Path
import sqlite3
import csv
from datetime import datetime, date, time, timedelta
import argparse

import logging
import python_logging_base
from python_logging_base import ASSERT

LOG = logging.getLogger("gen")
LOG.setLevel(logging.TRACE)

db = sqlite3.connect("./weather.db")
cursor = db.cursor()

fieldnames = [
    "id",
    "station_code",
    "datetime_dt",
    "temp_f",
    "dewpoint_f",
    "rel_humidity_pct",
    "heat_index_f",
    "wind_chill_f",
    "wind_direction_t",
    "wind_speed_mph",
    "wind_gust_mph",
    "visibility_m",
    "weather_t",
    "clouds_t",
    "pressure_sea_mb",
    "pressure_inhg",
    "altimiter_setting_inhg",
    "sixhr_max_f",
    "sixhr_min_f",
    "twentyfourhr_max_f",
    "twentyfourhr_min_f"
]

# We want these data:
# Today's temperature relative to the last few days' temperature
# First derivative data (i.e. how fast is it heating up)
# Integral data (i.e. heat capacity of the land or bay, depending on location)
# Historical probability data of some sort (At this time during the day, with this temperature, what is the odds for each of N temperatures)
# Same for integral
# Same for derivative
# Future: train a neural net with these vectors for prediction?

def fullRowsBetweenDates(start_date, end_date):
    query = f"""
    select *
    from weather
    where datetime_dt > ?
    and datetime_dt < ?
    order by datetime_dt
    """
    # TODO: this should be fairly easily cacheable if we get lots of data. It's used for a lot of the analyses.
    LOG.trace(f"Executing query: {query} for start_date={start_date} and end_date={end_date}")
    cursor.execute(query, (start_date, end_date))
    return cursor.fetchall()


class DayOverDay:
    def __init__(self):
        self._days = []
        self._lookback=7

    def process(self):
        today_morning_midnight = datetime.combine(date.today(), time.min)
        one_day_delta = timedelta(days=1)
        for daydiff in range(0, self._lookback):
            begin = today_morning_midnight - (daydiff * one_day_delta)
            end = today_morning_midnight - ((daydiff - 1) * one_day_delta)
            rows = fullRowsBetweenDates(begin, end)
            LOG.trace(f"Retrieved {len(rows)} rows for data between {begin} and {end}")
            self._days.append(rows)

    def to_csv(self):
        # OK, we don't know exactly what minute the measurements will be taken at, so we'll peek for the next smallest time, then pop all rows that are that time or before.
        def any_left():
            return any(len(row) > 0 for row in self._days)

        def min_time():
            min_time = None
            for day in self._days:
                if len(day) > 0: #we have any more rows
                    time = datetime.fromisoformat(day[0][2]).time()
                    if min_time is None or time < min_time:
                        min_time = time
            return min_time

        def row_date(row):
            return datetime.fromisoformat(row[2]).date()

        def row_time(row):
            return datetime.fromisoformat(row[2]).time()

        with open("./day_over_day.csv", "w") as f:
            fields = ["time"]
            for d in self._days:
                fields.append(row_date(d[0]).isoformat())
            writer = csv.DictWriter(f, fields)
            writer.writeheader()
            while any_left():
                mtime = min_time()
                LOG.debug(f"Processing {mtime}")
                time_entry = {
                    "time": mtime.isoformat()
                }
                for i, day in enumerate(self._days): #Don't need index maybe
                    if len(day) > 0 and row_time(day[0]) <= mtime:
                        row = day.pop(0)
                        temp_f = row[3]
                        time_entry[row_date(row).isoformat()] = temp_f
                LOG.info(f"Writing time entry: {time_entry}")
                writer.writerow(time_entry)

                

class DayOverDayDerivative:
    def __init__(self):
        pass

    def process(self):
        pass

    def to_csv(self):
        pass

class DayOverDayIntegral:
    def __init__(self):
        pass

    def process(self):
        pass

    def to_csv(self):
        pass

class ImputeRowV1:
    def __init__(self, db_row):
        self._db_row = db_row

    def imputed(self):
        breakpoint()

class MLTraining:
    def __init__(self):
        pass

    def process(self):
        pass

    def to_csv(self):
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-y', action='store_true', help='Process day-over-day sheet')
    parser.add_argument('-d', action='store_true', help='Process day-over-day derivative sheet')
    parser.add_argument('-i', action='store_true', help='Process day-over-day integral sheet')    
    parser.add_argument('-t', '--train', action='store_true', help='Train a neural network model on the data')
    args = parser.parse_args()

    processors = []

    if args.y:
        LOG.trace("Day over day flag given")
        processors.append(DayOverDay())
    if args.d:
        LOG.trace("Day over day derivative flag given")
        processors.append(DayOverDayDerivative())
    if args.i:
        LOG.trace("Day over day integral flag given")
        processors.append(DayOverDayIntegral())
    if args.train:
        LOG.trace("Training a neural network model")
        processors.append(MLTraining())



    for p in processors:
        p.process()
        p.to_csv()