import json
from datetime import datetime, time, timedelta
from pathlib import Path

import pandas as pd
from dateutil.parser import parse as dateutil_parse

Date = str | datetime

SCHEMA_DIR = Path(__file__).parent / "data" / "schemas"
GDELT_V2_BASE_URL = "http://data.gdeltproject.org/gdeltv2/"
GDELT_V2_START = datetime(2015, 2, 18)


def load_json(
    json_message: bytes | str,
    max_recursion_depth: int = 100,
    recursion_depth: int = 0,
) -> dict:
    """
    Load JSON string, removing offending characters if present.

    Parameters
    ----------
    json_message
        The JSON string or bytes to parse.
    max_recursion_depth
        Maximum recursion depth for error recovery.
    recursion_depth
        Current recursion depth (internal use).

    Returns
    -------
    dict
        The parsed JSON object.

    Raises
    ------
    ValueError
        If max recursion depth is reached.
    """
    try:
        message_str = json_message.decode() if isinstance(json_message, bytes) else json_message
        return json.loads(message_str)
    except ValueError as e:
        if recursion_depth >= max_recursion_depth:
            raise ValueError("Max recursion depth reached while parsing JSON.") from e
        message_str = json_message.decode() if isinstance(json_message, bytes) else json_message
        idx_to_replace = int(e.args[0].split(" ")[-1][:-1])
        fixed_message = message_str[:idx_to_replace] + " " + message_str[idx_to_replace + 1 :]
        return load_json(fixed_message, max_recursion_depth, recursion_depth + 1)


def format_date(date: Date) -> str:
    """
    Format date for GDELT DOC API (YYYYMMDDHHMMSS format).

    Parameters
    ----------
    date
        Date as string (YYYY-MM-DD) or datetime object.

    Returns
    -------
    str
        Date formatted as YYYYMMDDHHMMSS.
    """
    if isinstance(date, str):
        return f"{date.replace('-', '')}000000"
    if isinstance(date, datetime):
        return date.strftime("%Y%m%d%H%M%S")
    raise ValueError(f"Unsupported type for date: {type(date)}")


def get_15min_intervals() -> list[str]:
    """
    Generate all 15-minute time intervals for a day.

    Returns
    -------
    list[str]
        List of time strings in HHMMSS format from 000000 to 234500.
    """
    return [f"{h:02d}{m:02d}00" for h in range(24) for m in (0, 15, 30, 45)]


def parse_date(date_input: str | datetime) -> datetime:
    """
    Parse date string or pass through datetime.

    Parameters
    ----------
    date_input
        Date as string (various formats supported) or datetime object.

    Returns
    -------
    datetime
        Parsed datetime object.

    Raises
    ------
    ValueError
        If the date string cannot be parsed.
    """
    if isinstance(date_input, datetime):
        return date_input
    try:
        return dateutil_parse(date_input)
    except Exception as e:
        raise ValueError(f"Cannot parse date: {date_input}") from e


def date_range(start: str | datetime, end: str | datetime) -> list[datetime]:
    """
    Generate list of dates from start to end (inclusive).

    Parameters
    ----------
    start
        Start date.
    end
        End date.

    Returns
    -------
    list[datetime]
        List of datetime objects for each day in the range.
    """
    start_dt = parse_date(start)
    end_dt = parse_date(end)
    dates: list[datetime] = []
    current = datetime.combine(start_dt.date(), time.min)
    end_date = end_dt.date()
    while current.date() <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def expand_dates(
    date: str | datetime | list[str | datetime],
    coverage: bool = False,
) -> list[str]:
    """
    Expand date input to list of GDELT-formatted date strings.

    Parameters
    ----------
    date
        Single date, two-element list [start, end], or list of specific dates.
    coverage
        If True, expand each day to all 15-minute intervals.

    Returns
    -------
    list[str]
        List of date strings in YYYYMMDDHHMMSS format.
    """
    now = datetime.now()
    intervals = get_15min_intervals()

    if isinstance(date, str | datetime):
        dates = [parse_date(date)]
    elif isinstance(date, list):
        dates = date_range(date[0], date[1]) if len(date) == 2 else [parse_date(d) for d in date]
    else:
        raise ValueError(f"Unsupported date type: {type(date)}")

    result: list[str] = []
    for dt in dates:
        date_str = dt.strftime("%Y%m%d")
        if coverage:
            if dt.date() == now.date():
                current_interval = (now.hour * 4) + (now.minute // 15)
                day_intervals = intervals[: current_interval + 1]
            else:
                day_intervals = intervals
            result.extend(f"{date_str}{interval}" for interval in day_intervals)
        else:
            if dt.date() == now.date():
                minute_interval = (now.minute // 15) * 15
                adjusted = now.replace(minute=minute_interval, second=0, microsecond=0)
                adjusted -= timedelta(minutes=15)
                result.append(adjusted.strftime("%Y%m%d%H%M%S"))
            else:
                result.append(f"{date_str}234500")
    return result


def load_schema(table: str) -> list[str]:
    """
    Load column headers from local schema files.

    Parameters
    ----------
    table
        Table name: 'events', 'mentions', or 'gkg'.

    Returns
    -------
    list[str]
        List of column names for the table.

    Raises
    ------
    ValueError
        If table name is not recognized.
    """
    schema_files = {
        "events": "eventsv2.json",
        "mentions": "mentions.json",
        "gkg": "gkgv2.json",
    }

    if table not in schema_files:
        raise ValueError(f"Unknown table: {table}. Must be one of: {list(schema_files.keys())}")

    schema_path = SCHEMA_DIR / schema_files[table]
    with schema_path.open() as f:
        schema_data = json.load(f)

    return [field["name"] for field in schema_data["schema"]["fields"]]


def load_cameo_codes() -> pd.DataFrame:
    """
    Load CAMEO codes lookup table from local JSON.

    Returns
    -------
    pd.DataFrame
        DataFrame with CAMEO codes indexed by code.
    """
    cameo_path = SCHEMA_DIR / "cameoCodes.json"
    codes = pd.read_json(
        cameo_path,
        dtype={"cameoCode": "str", "GoldsteinScale": float},
        precise_float=True,
        convert_dates=False,
    )
    codes.set_index("cameoCode", drop=False, inplace=True)
    return codes


def get_cameo_description(code: str, codes_df: pd.DataFrame) -> str:
    """
    Look up CAMEO code description.

    Parameters
    ----------
    code
        CAMEO event code.
    codes_df
        DataFrame with CAMEO codes (from load_cameo_codes).

    Returns
    -------
    str
        Description of the CAMEO code.
    """
    try:
        desc = codes_df.loc[code, "Description"]
        return str(desc) if desc is not None else f"No description for CAMEO code {code}"
    except (KeyError, TypeError):
        return f"No description for CAMEO code {code}"
