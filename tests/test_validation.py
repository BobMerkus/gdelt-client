from datetime import datetime, timedelta

import pytest

from gdelt_client.enums import GdeltTable
from gdelt_client.validation import validate_date, validate_table, validate_tone


class TestValidateTone:
    def test_valid_tone_doesnt_raise_error(self):
        validate_tone(">5")

    def test_raises_when_comparator_missing(self):
        with pytest.raises(ValueError):
            validate_tone("10")

    def test_raises_when_equals_in_comparator(self):
        with pytest.raises(ValueError):
            validate_tone(">=10")

    def test_raises_when_multiple_tones(self):
        with pytest.raises(ValueError):
            validate_tone([">5", "<10"])


class TestValidateDate:
    def test_valid_date_doesnt_raise(self):
        validate_date("2020-01-15")

    def test_valid_datetime_doesnt_raise(self):
        validate_date(datetime(2020, 1, 15))

    def test_valid_date_range_doesnt_raise(self):
        validate_date(["2020-01-15", "2020-01-20"])

    def test_raises_for_future_date(self):
        future = datetime.now() + timedelta(days=10)
        with pytest.raises(ValueError, match="in the future"):
            validate_date(future)

    def test_raises_for_date_before_gdelt_v2_start(self):
        with pytest.raises(ValueError, match=r"before GDELT 2.0 start"):
            validate_date("2010-01-01")

    def test_raises_for_invalid_date_range(self):
        with pytest.raises(ValueError, match="must be before end date"):
            validate_date(["2020-01-20", "2020-01-15"])

    def test_raises_for_invalid_type(self):
        with pytest.raises(ValueError, match="Unsupported date type"):
            validate_date(12345)


class TestValidateTable:
    def test_valid_table_string_doesnt_raise(self):
        validate_table("events")
        validate_table("mentions")
        validate_table("gkg")

    def test_valid_table_enum_doesnt_raise(self):
        validate_table(GdeltTable.EVENTS)
        validate_table(GdeltTable.MENTIONS)
        validate_table(GdeltTable.GKG)

    def test_raises_for_invalid_table(self):
        with pytest.raises(ValueError, match="Invalid table"):
            validate_table("invalid_table")
