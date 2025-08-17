# ---------- helpers functions ----------

import json

# ---------- Reading Data ----------
def parse_json(x):
    """Return dict from dict/JSON string/bytes; raise if not parseable."""
    if isinstance(x, dict):
        return x
    if isinstance(x, (bytes, bytearray)):
        x = x.decode("utf-8", errors="ignore")
    if isinstance(x, str):
        return json.loads(x)  # let it raise if malformed
    raise ValueError("Not JSON-like")

def normkey(k: str) -> str:
    """Normalize keys: lowercase, strip, remove non-alnum."""
    return "".join(ch for ch in k.lower().strip() if ch.isalnum())

def rget(d: dict, *names):
    """Relaxed get: try direct keys, then normalized-key matches."""
    if not isinstance(d, dict):
        return None
    # match to direct key
    for name in names:
        if name in d:
            return d[name]
    # match to normalized key
    nd = {normkey(k): v for k, v in d.items()}
    for name in names:
        v = nd.get(normkey(name))
        if v is not None or normkey(name) in nd:
            return v
    return None

def to_num(x):
    """Convert to float if possible; keep ints as ints; else None."""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return x
    s = str(x).strip()
    if s == "":
        return None
    try:
        # remove thousands separators if any
        return float(s.replace(",", ""))
    except Exception:
        return None

def flatten_row(item):
    """item is usually (weather_json, date_json, station_json)."""
    if not isinstance(item, (list, tuple)) or len(item) < 3:
        # try dict shape fallback
        if isinstance(item, dict):
            w = parse_json(rget(item, "weather", "Weather", "w", "data"))
            d = parse_json(rget(item, "date", "Date", "d"))
            s = parse_json(rget(item, "station", "Station", "s"))
        else:
            # last-ditch: treat entire item as weather
            w, d, s = parse_json(item), {}, {}
    else:
        w = parse_json(item[0])
        d = parse_json(item[1])
        s = parse_json(item[2])

    temp = rget(w, "Temperature") or {}
    wind = rget(w, "Wind") or {}

    return {
        "precipitation":  to_num(rget(w, "Precipitation")),
        "avg_temp":       to_num(rget(temp, "Avg Temp", "AvgTemp", "Average", "Avg")),
        "max_temp":       to_num(rget(temp, "Max Temp", "MaxTemp", "Max")),
        "min_temp":       to_num(rget(temp, "Min Temp", "MinTemp", "Min")),
        "wind_direction": to_num(rget(wind, "Direction", "Dir")),
        "wind_speed":     to_num(rget(wind, "Speed", "WindSpeed", "Speed(mph)")),
        "date_full":      rget(d, "Full", "Date", "DateFull"),
        "year":           to_num(rget(d, "Year")),
        "month":          to_num(rget(d, "Month")),
        "week_of":        to_num(rget(d, "Week of", "WeekOf", "Week")),
        "city":           rget(s, "City"),
        "code":           rget(s, "Code", "StationCode"),
        "location":       rget(s, "Location"),
        "state":          rget(s, "State"),
    }
