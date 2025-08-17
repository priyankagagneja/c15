"""
Reading and parsing data

This script reads and parses the data from a pickle file and saves 
it as a CSV file
"""

import pickle, csv      
# These libraries are part of python 3.11 standard library, 
# so they dont need to be installed (thus exclude from .toml). 
# Only load them win them in the environment.

# import from the script utils.py
from src.utils import flatten_row

INPUT_PATH = "data/weather.data"
OUTPUT_CSV = "data/weather_parsed.csv"

with open(INPUT_PATH, "rb") as f:
    obj = pickle.load(f)

# Normalize into an iterable of rows
if isinstance(obj, (list, tuple)):
    iterable = obj
elif isinstance(obj, dict):
    # try to interpret as dict-of-lists
    try:
        keys = list(obj.keys())
        n = len(next(iter(obj.values())))
        iterable = [tuple(obj[k][i] for k in keys) for i in range(n)]
    except Exception:
        iterable = [obj]
else:
    iterable = [obj]

fields = [
    "precipitation","avg_temp","max_temp","min_temp",
    "wind_direction","wind_speed",
    "date_full","year","month","week_of",
    "city","code","location","state"
]

rows_written = 0
skipped = 0

with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fields)
    w.writeheader()
    for item in iterable:
        try:
            rec = flatten_row(item)       # flatten_row() from utils.py

            # Write the row even if some fields are None; zeros (0) are preserved.
            w.writerow({k: ("" if rec[k] is None else rec[k]) for k in fields})
            rows_written += 1

        except Exception:
            skipped += 1

print(f"Wrote {OUTPUT_CSV} with {rows_written} rows. Skipped: {skipped}.")
