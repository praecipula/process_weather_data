#!/usr/bin/env python
import cmd2
from cmd2 import with_argparser
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import sqlite3
import re
import json

import logging
import python_logging_base
from python_logging_base import ASSERT

LOG = logging.getLogger("cli")


here = Path(".")

# One unified weather database. Each data is in the same table but split by station_code
# This means we have to have a unique index, hmm. Better to shard to separate dbs by station?

db = sqlite3.connect("./weather.db")

cursor = db.cursor()

cursor.execute("""CREATE TABLE IF NOT EXISTS summary_fcst (
    id INTEGER PRIMARY KEY,
    station_code TEXT NOT NULL,
    date_d TEXT NOT NULL,
    max_temp_f INTEGER,
    max_temp_t TEXT,
    max_temp_record INTEGER,
    max_temp_normal INTEGER,
    min_temp_f INTEGER,
    min_temp_t TEXT,
    min_temp_record INTEGER,
    min_temp_normal INTEGER,
    precip_in REAL,
    precip_mtd_in REAL,
    precip_mtd_normal_in REAL,
    average_wind_mph REAL,
    gust_wind_mph REAL,
    sky_cover_pct REAL,
    weather_cond_arr TEXT,
    sunrise_t TEXT,
    sunset_t TEXT
  )""")

cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_stn_time_summary ON summary_fcst (station_code, date_d)
  """)


class NWSReportParser:
    def __init__(self, filename):
        # If filename is none, assume we've got the data in the clipboard?
        with open(filename, 'r') as f:
            self._report_txt = f.read().strip()
        self._map = {}

    def parse_to_map(self):
        # First, check that we have a *final* report.

        if len(re.findall(r"VALID TODAY AS OF", self._report_txt)) > 0:
            LOG.warning(f"This appears to be a preliminary weather report! Try the next chronological to get the \"yesterday\"-finalized data.")
            return None
        self._map["station_code"] = self.station_code()
        self._map["date_d"] = self.date()
        self._map["max_temp_f"] = self.max_temp()
        self._map["max_temp_t"] = self.max_time()
        self._map["max_temp_record"] = self.max_record()
        self._map["max_temp_normal"] = self.max_normal()
        self._map["min_temp_f"] = self.min_temp()
        self._map["min_temp_t"] = self.min_time()
        self._map["min_temp_record"] = self.min_record()
        self._map["min_temp_normal"] = self.min_normal()
        self._map["precip_in"] = self.precipitation()
        self._map["precip_mtd_in"] = self.precipitation_mtd()
        self._map["precip_mtd_normal_in"] = self.precipitation_mtd_normal()
        self._map["average_wind_mph"] = self.average_wind()
        self._map["gust_wind_mph"] = self.gust_wind()
        self._map["sky_cover_pct"] = self.sky_cover_pct()
        self._map["weather_cond_arr"] = json.dumps(self.weather_conditions())
        # TODO: No reason this needs to be a member... it's all in this function.
        return self._map

    def parse_today_to_map(self):
        # First, check that we have a *final* report.
        if len(re.findall(r"VALID TODAY AS OF", self._report_txt)) > 0:
            LOG.warning(f"This appears to be a preliminary weather report! Try the next chronological to get the \"yesterday\"-finalized data.")
            return None
        today_map = {}
        today_map["max_temp_normal"] = self.today_max_normal()
        today_map["max_temp_record"] = self.today_max_record()
        today_map["min_temp_normal"] = self.today_min_normal()
        today_map["min_temp_record"] = self.today_min_record()
        today_map["sunrise_t"] = self.today_sunrise()
        today_map["sunset_t"] = self.today_sunset()
        return today_map

        
    
    def _find_multiple(self, regex, field):
        found = re.findall(regex, self._report_txt, re.MULTILINE)
        if len(found) == 0:
            LOG.warning(f"Found 0 {field if field else '[no_field]'}: {found}")
            breakpoint()
        return [f.strip() for f in found]

    def _find_one(self, regex, field):
        found = self._find_multiple(regex, field)
        if not len(found) == 1:
            LOG.warning(f"Found 0 or more than 1 {field if field else '[no_field]'}: {found}")
            breakpoint()
        return found[0]

    def station_code(self):
        station = self._find_one(r"CLI[A-Z0-9]*\n", "code")
        if station == "CLISFO":
            return "KSFO"

    def date(self):
        datestr = self._find_one(r"CLIMATE SUMMARY FOR(.*)\n", "date")
        datestr = datestr.replace(".", "")
        dt = datetime.strptime(datestr, "%B %d %Y")
        return dt

    def max_temp(self):
        # MAXIMUM, then any string of 1 or more digits with optional R (record)
        return self._find_one(r"\s*MAXIMUM\s*(\d+)R?", "max temp")

    def max_time(self):
        # MAXIMUM, then some spaces, some digits, some spaces, then a number colon space finished by AM or PM
        return self._find_one(r"\s*MAXIMUM\s*\d+R?\s*([0-9:\s]+(?:AM|PM))", "max temp time")

    def max_record(self):
        # MAXIMUM then any chars; then date, then some spaces, then digits.
        return self._find_one(r"\s*MAXIMUM.*[0-9:\s]+(?:AM|PM)\s+(\d+)", "max record")

    def max_normal(self):
        # MAXIMUM, then anything, a timestamp, anything, 4 digits (a year), and then the next string of digits.
        return self._find_one(r"\s*MAXIMUM.*[0-9:\s]+(?:AM|PM).*\d{4}\s+(\d+)", "max normal")
    
    def min_temp(self):
        return self._find_one(r"\s*MINIMUM\s*(\d+)R?", "min temp")

    def min_time(self):
        # MINIMUM, then some spaces, some digits, some spaces, then a number colon space finished by AM or PM
        return self._find_one(r"\s*MINIMUM\s*\d+R?\s*([0-9:\s]+(?:AM|PM))", "min temp time")

    def min_record(self):
        # MINIMUM then any chars; then date, then some spaces, then digits.
        return self._find_one(r"\s*MINIMUM.*[0-9:\s]+(?:AM|PM)\s+(\d+)", "min record")

    def min_normal(self):
        # MINIMUM, then anything, a timestamp, anything, 4 digits (a year), and then the next string of digits.
        return self._find_one(r"\s*MINIMUM.*[0-9:\s]+(?:AM|PM).*\d{4}\s+(\d+)", "min normal")

    def precipitation(self):
        # Precipitation header, then yesterday, then any spaces and first cluster of digits
        return self._find_one(r"PRECIPITATION.*\n\s+YESTERDAY\s+([\d\.]+)", "precipitation")

    def precipitation_mtd(self):
        # Strategy as above, with a skipped line.
        return self._find_one(r"PRECIPITATION.*\n.*\n\s+MONTH TO DATE\s+([\d\.]+)", "precipitation mtd")

    def precipitation_mtd_normal(self):
        # As above, but consuming one extra digit/dot set.
        return self._find_one(r"PRECIPITATION.*\n.*\n\s+MONTH TO DATE\s+[\d\.]+\s+([\d\.]+)", "precipitation mtd normal")

    def average_wind(self):
        # Note the \s\S - match anything a space or not a space means match across multiple lines
        return self._find_one(r"WIND[\s\S]+AVERAGE WIND SPEED\s+([\d.]+)", "average wind")

    def gust_wind(self):
        return self._find_one(r"WIND[\s\S]+HIGHEST GUST SPEED\s+([\d.]+)", "gust wind")

    def sky_cover_pct(self):
        return self._find_one(r"SKY COVER[\s\S]+AVERAGE SKY COVER\s+([\d.]+)", "sky cover")

    def weather_conditions(self):
        # Non-regex based split on token and read off lines.
        lines = self._report_txt.split("\n") 
        idx = lines.index("WEATHER CONDITIONS")
        line = lines[idx].strip()
        token_set = []
        while len(line) != 0:
            try:
                space_found = line.index(" ")
                # Skip any line that parses a space.
                # This will definitely do the WEATHER CONDITIONS line but keep going from there.
            except ValueError:
                # This is actually the one we want!
                token_set.append(line)
            finally:
                idx = idx + 1
                line = lines[idx].strip()
        return token_set

    def today_max_normal(self):
        # Special note: see \D as "not a digit"
        return self._find_one(r"CLIMATE NORMALS FOR TODAY[\s\S]+MAXIMUM\D+(\d+)", "today max normal")

    def today_max_record(self):
        return self._find_one(r"CLIMATE NORMALS FOR TODAY[\s\S]+MAXIMUM\D+\d+\D+(\d+)", "today max record")

    def today_min_normal(self):
        # Special note: see \D as "not a digit"
        return self._find_one(r"CLIMATE NORMALS FOR TODAY[\s\S]+MINIMUM\D+(\d+)", "today min normal")

    def today_min_record(self):
        return self._find_one(r"CLIMATE NORMALS FOR TODAY[\s\S]+MINIMUM\D+\d+\D+(\d+)", "today min record")

    def today_sunrise(self):
        return self._find_one(r"SUNRISE AND SUNSET\s*\n.*SUNRISE\s+([0-9:\s]+(?:AM|PM))", "today sunrise")

    def today_sunset(self):
        return self._find_one(r"SUNRISE AND SUNSET\s*\n.*SUNSET\s+([0-9:\s]+(?:AM|PM))", "today sunset")







class ResultsApp(cmd2.Cmd):
    """Interactive CLI for managing weather forecast data."""


    def __init__(self):
        super().__init__(persistent_history_file = "./manual_entry_history")

        # Hide built-in commands we don't need for a cleaner help menu
        self.hidden_commands.extend(["shell", "edit", "py", "run_script"])
        self.intro = (
            "Welcome to the Results Tracker!\n"
            "Type 'help' for available commands, or 'quit' to exit.\n"
        )
        self.prompt = "🌤️: "

    # -------------------------------------------------------------------------
    # add_result
    # -------------------------------------------------------------------------

    add_result_parser = argparse.ArgumentParser(description="Add a real next-day result summary.")
    add_result_parser.add_argument("--station", type=str, help="Station code")
    add_result_parser.add_argument("--date", type=str, help="Date in iso, e.g. 2026-03-27")
    add_result_parser.add_argument("--high", type=int, help="High temperature")
    add_result_parser.add_argument("--high_t", type=str, help="Time of high temperature")
    add_result_parser.add_argument("--high_record", type=int, help="Record high temp for this day")
    add_result_parser.add_argument("--high_normal", type=int, help="Normal high for this day")
    add_result_parser.add_argument("--low", type=int, help="Low temperature")
    add_result_parser.add_argument("--low_t", type=str, help="Time of high temperature")
    add_result_parser.add_argument("--low_record", type=int, help="Record high temp for this day")
    add_result_parser.add_argument("--low_normal", type=int, help="Normal high for this day")
    add_result_parser.add_argument("--sky_cover", type=float, help="Sky cover on this day, 0..1")
    add_result_parser.add_argument("--sunrise_t", type=str, help="Sunrise datetime")
    add_result_parser.add_argument("--sunset_t", type=str, help="Sunset datetime")

    @with_argparser(add_result_parser)
    def do_add_result(self, args):
        """Add a match result for a team."""
        def d_roundtrip(argstr):
            """Convenience function simply round-trips the date.
            This way we can be sure that we have a consistent text entry
            in the database."""
            return datetime.strptime(argstr, "%Y-%m-%d").strftime("%Y-%m-%d")
        def t_roundtrip(argstr):
            """Convenience function simply round-trips the time.
            This way we can be sure that we have a consistent text entry
            in the database."""
            try:
                return datetime.strptime(argstr, "%H:%M").strftime("%H:%M:%S")
            except ValueError:
                pass # Not this format
            return datetime.strptime(argstr, "%H:%M:%S").strftime("%H:%M:%S")

        args_mapping = {
            "station_code": args.station,
            "date_d": d_roundtrip(args.date),
            "max_temp_f": args.high,
            "max_temp_t": t_roundtrip(args.high_t),
            "max_temp_record": args.high_record,
            "max_temp_normal": args.high_normal,
            "min_temp_f": args.low,
            "min_temp_t": t_roundtrip(args.low_t),
            "min_temp_record": args.low_record,
            "min_temp_normal": args.low_normal,
            "sky_cover_pct": args.sky_cover,
            "sunrise_t": t_roundtrip(args.sunrise_t),
            "sunset_t": t_roundtrip(args.sunset_t)
        }
        db_statement = f"""
        INSERT INTO summary_fcst ({', '.join(args_mapping.keys())})
        VALUES ({('?, ' * len(args_mapping.keys()))[:-2]})
        ON CONFLICT (station_code, date_d) DO NOTHING"""
        LOG.debug(f"Saving map: {args_mapping}")
        cursor.execute(db_statement, tuple(args_mapping.values()))
        db.commit()
        if cursor.rowcount == 0:
            LOG.debug(f"Skipping existing dt in db: {args_mapping['station_code']} @ {args_mapping['date_d']}")
        else:
            LOG.debug(f"Inserted row!")

    def do_q(self, _):
        """Quits the shell"""
        return True

    def do_parse(self, _):
        file = "./weather_report.txt"
        p = NWSReportParser(file)
        yesterday_map = p.parse_to_map()

        # Upsert on fail. This is because we can prepopulate forecast for
        # tomorrow, and on tomorrow we want to in fact upsert actuals.
        keys_to_filter = ["station_code", "date_d"]
        upsert_map = {k: yesterday_map[k] for k in yesterday_map.keys() if k not in keys_to_filter}
        # We can drop the values because of the special EXCLUDED.(conflict) entries that are saved in the sql clause.
        upsert_statement = ", ".join([f"{k} = EXCLUDED.{k}" for k in upsert_map.keys()])

        db_statement = f"""
        INSERT INTO summary_fcst ({', '.join(yesterday_map.keys())})
        VALUES ({('?, ' * len(yesterday_map.keys()))[:-2]})
        ON CONFLICT (station_code, date_d) DO 
        UPDATE SET
        {upsert_statement}"""

        LOG.debug(f"Saving map: {yesterday_map}")
        cursor.execute(db_statement, tuple(yesterday_map.values()))
        db.commit()
        if cursor.rowcount == 0:
            LOG.debug(f"Skipping existing dt in db: {yesterday_map['station_code']} @ {yesterday_map['date_d']}")
        else:
            LOG.debug(f"Inserted row!")

        # Also create predicted entries for today

        today_map = p.parse_today_to_map()
        today_map["date_d"] = yesterday_map["date_d"] + timedelta(days=1)
        today_map["station_code"] = yesterday_map["station_code"]

        upsert_map = {k: today_map[k] for k in today_map.keys() if k not in keys_to_filter}
        # We can drop the values because of the special EXCLUDED.(conflict) entries that are saved in the sql clause.
        upsert_statement = ", ".join([f"{k} = EXCLUDED.{k}" for k in upsert_map.keys()])


        db_statement = f"""
        INSERT INTO summary_fcst ({', '.join(today_map.keys())})
        VALUES ({('?, ' * len(today_map.keys()))[:-2]})
        ON CONFLICT (station_code, date_d) DO
        UPDATE SET
        {upsert_statement}"""
        cursor.execute(db_statement, tuple(today_map.values()))
        db.commit()
        if cursor.rowcount == 0:
            LOG.debug(f"Skipping existing dt in db: {today_map['station_code']} @ {today_map['date_d']}")
        else:
            LOG.debug(f"Inserted row!")





if __name__ == "__main__":
    app = ResultsApp()
    app.cmdloop()