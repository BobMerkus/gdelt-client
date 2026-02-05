from datetime import datetime

from gdelt_client.enums import GdeltTable
from gdelt_client.helpers import GDELT_V2_START, parse_date

Filter = list[str] | str


def validate_tone(tone: Filter) -> None:
    """
    Validate tone filter format.

    Parameters
    ----------
    tone
        Tone filter string (e.g., ">5" or "<-5").

    Raises
    ------
    ValueError
        If tone doesn't contain < or >, or contains =.
    NotImplementedError
        If multiple tones are provided.
    """
    if not ("<" in tone or ">" in tone):
        raise ValueError("Tone must contain either greater than or less than")

    if "=" in tone:
        raise ValueError("Tone cannot contain '='")

    if isinstance(tone, list):
        raise NotImplementedError("Multiple tones are not supported yet")


def validate_date(date: str | datetime | list[str | datetime]) -> None:
    """
    Validate date(s) for GDELT 2.0 constraints.

    Parameters
    ----------
    date
        Single date, date range, or list of dates.

    Raises
    ------
    ValueError
        If any date is in the future or before GDELT 2.0 start date.
    """
    now = datetime.now()

    if isinstance(date, str | datetime):
        dates = [parse_date(date)]
    elif isinstance(date, list):
        dates = [parse_date(d) for d in date]
    else:
        raise ValueError(f"Unsupported date type: {type(date)}")

    for dt in dates:
        if dt > now:
            raise ValueError(f"Date {dt} is in the future. Please enter a valid date.")

        if dt < GDELT_V2_START:
            raise ValueError(
                f"Date {dt} is before GDELT 2.0 start date ({GDELT_V2_START.date()}). "
                "GDELT 2.0 only supports dates from Feb 18, 2015 onwards."
            )

    if len(dates) == 2 and dates[0] >= dates[1]:
        raise ValueError(f"Start date ({dates[0]}) must be before end date ({dates[1]}).")


def validate_table(table: GdeltTable | str, translation: bool = False) -> None:
    """
    Validate table configuration for GDELT 2.0.

    Parameters
    ----------
    table
        Table name: 'events', 'mentions', or 'gkg'.
    translation
        Whether translation data is requested.

    Raises
    ------
    ValueError
        If table name is not valid.
    """
    valid_tables = {t.value for t in GdeltTable}
    table_str = table.value if isinstance(table, GdeltTable) else table

    if table_str not in valid_tables:
        raise ValueError(f"Invalid table '{table_str}'. Must be one of: {', '.join(valid_tables)}")
