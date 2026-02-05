from datetime import datetime

import pandas as pd
import pytest

from gdelt_client.helpers import (
    date_range,
    expand_dates,
    format_date,
    get_15min_intervals,
    get_cameo_description,
    load_cameo_codes,
    load_schema,
    parse_date,
)


class TestFormatDate:
    def test_returns_string_input(self):
        date = "2020-01-01"
        assert format_date(date) == "20200101000000"

    def test_converts_a_datetime_to_string_in_correct_format(self):
        date = datetime(year=2020, month=1, day=1, hour=12, minute=30, second=30)
        assert format_date(date) == "20200101123030"

    def test_raises_for_invalid_type(self):
        with pytest.raises(ValueError, match="Unsupported type"):
            format_date(12345)


class TestGet15MinIntervals:
    def test_returns_96_intervals(self):
        intervals = get_15min_intervals()
        assert len(intervals) == 96

    def test_first_interval_is_midnight(self):
        intervals = get_15min_intervals()
        assert intervals[0] == "000000"

    def test_last_interval_is_2345(self):
        intervals = get_15min_intervals()
        assert intervals[-1] == "234500"


class TestParseDate:
    def test_parses_iso_string(self):
        result = parse_date("2020-01-15")
        assert result.year == 2020
        assert result.month == 1
        assert result.day == 15

    def test_passes_datetime_through(self):
        dt = datetime(2020, 6, 15, 12, 30)
        result = parse_date(dt)
        assert result == dt

    def test_raises_for_invalid_string(self):
        with pytest.raises(ValueError, match="Cannot parse date"):
            parse_date("not-a-date")


class TestDateRange:
    def test_returns_single_day_for_same_date(self):
        result = date_range("2020-01-15", "2020-01-15")
        assert len(result) == 1
        assert result[0].date() == datetime(2020, 1, 15).date()

    def test_returns_inclusive_range(self):
        result = date_range("2020-01-15", "2020-01-17")
        assert len(result) == 3

    def test_accepts_datetime_objects(self):
        start = datetime(2020, 1, 15)
        end = datetime(2020, 1, 17)
        result = date_range(start, end)
        assert len(result) == 3


class TestExpandDates:
    def test_single_date_without_coverage(self):
        result = expand_dates("2020-01-15")
        assert len(result) == 1
        assert result[0] == "20200115234500"

    def test_date_range_without_coverage(self):
        result = expand_dates(["2020-01-15", "2020-01-16"])
        assert len(result) == 2

    def test_single_date_with_coverage(self):
        result = expand_dates("2020-01-15", coverage=True)
        assert len(result) == 96
        assert result[0] == "20200115000000"
        assert result[-1] == "20200115234500"

    def test_raises_for_invalid_type(self):
        with pytest.raises(ValueError, match="Unsupported date type"):
            expand_dates(12345)


class TestLoadSchema:
    def test_loads_events_schema(self):
        columns = load_schema("events")
        assert isinstance(columns, list)
        assert len(columns) > 0
        assert "GLOBALEVENTID" in columns

    def test_loads_mentions_schema(self):
        columns = load_schema("mentions")
        assert isinstance(columns, list)
        assert len(columns) > 0

    def test_loads_gkg_schema(self):
        columns = load_schema("gkg")
        assert isinstance(columns, list)
        assert len(columns) > 0

    def test_raises_for_unknown_table(self):
        with pytest.raises(ValueError, match="Unknown table"):
            load_schema("invalid_table")


class TestLoadCameoCodes:
    def test_returns_dataframe(self):
        codes = load_cameo_codes()
        assert isinstance(codes, pd.DataFrame)

    def test_has_expected_columns(self):
        codes = load_cameo_codes()
        assert "cameoCode" in codes.columns
        assert "Description" in codes.columns


class TestGetCameoDescription:
    def test_returns_description_for_valid_code(self):
        codes = load_cameo_codes()
        desc = get_cameo_description("01", codes)
        assert isinstance(desc, str)
        assert "No description" not in desc

    def test_returns_fallback_for_invalid_code(self):
        codes = load_cameo_codes()
        desc = get_cameo_description("INVALID", codes)
        assert "No description" in desc
