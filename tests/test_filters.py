from datetime import datetime

import pytest

from gdelt_client import (
    VALID_TIMESPAN_UNITS,
    Filters,
    multi_near,
    multi_repeat,
    near,
    repeat,
)


class TestFilters:
    """
    Test that the correct query strings are generated from
    various filters.
    """

    def test_single_keyword_filter(self):
        f = Filters(keyword="airline", start_date="2020-03-01", end_date="2020-03-02")
        assert f.query_string == ('"airline" &startdatetime=20200301000000&enddatetime=20200302000000&maxrecords=250')

    def test_single_keyphrase_filter(self):
        f = Filters(keyword="climate change", start_date="2020-03-01", end_date="2020-03-02")
        assert f.query_string == (
            '"climate change" &startdatetime=20200301000000&enddatetime=20200302000000&maxrecords=250'
        )

    def test_multiple_keywords(self):
        f = Filters(
            keyword=["airline", "climate"],
            start_date="2020-05-13",
            end_date="2020-05-14",
        )
        assert f.query_string == (
            "(airline OR climate) &startdatetime=20200513000000&enddatetime=20200514000000&maxrecords=250"
        )

    def test_multiple_themes(self):
        f = Filters(
            theme=["ENV_CLIMATECHANGE", "LEADER"],
            start_date="2020-05-13",
            end_date="2020-05-14",
        )
        assert f.query_string == (
            "(theme:ENV_CLIMATECHANGE OR theme:LEADER) &startdatetime=20200513000000&"
            "enddatetime=20200514000000&maxrecords=250"
        )

    def test_theme_and_keyword(self):
        f = Filters(
            keyword="airline",
            theme="ENV_CLIMATECHANGE",
            start_date="2020-05-13",
            end_date="2020-05-14",
        )
        assert f.query_string == (
            '"airline" theme:ENV_CLIMATECHANGE &startdatetime=20200513000000&enddatetime=20200514000000&maxrecords=250'
        )

    def test_tone_filter(self):
        f = Filters(
            keyword="airline",
            start_date="2020-03-01",
            end_date="2020-03-02",
            tone=">10",
        )
        assert f.query_string == (
            '"airline" tone>10 &startdatetime=20200301000000&enddatetime=20200302000000&maxrecords=250'
        )

    def test_start_date_as_datetime_is_formatted_as_expected(self):
        f = Filters(
            keyword="airline",
            start_date=datetime(year=2020, month=3, day=1),
            end_date=datetime(year=2020, month=3, day=2),
        )
        assert f.query_string == ('"airline" &startdatetime=20200301000000&enddatetime=20200302000000&maxrecords=250')


class TestNear:
    """
    Test that `near()` generates the right filters and errors.
    """

    def test_two_words(self):
        assert near(5, "airline", "crisis") == 'near5:"airline crisis" '

    def test_three_words(self):
        assert near(10, "airline", "climate", "change") == 'near10:"airline climate change" '

    def test_one_word(self):
        with pytest.raises(ValueError, match="At least two words"):
            near(5, "airline")


class TestMultiNear:
    """
    Test that `multi_near()` generates the right filters and errors.
    """

    def test_single_near(self):
        assert multi_near([(5, "airline", "crisis")]) == near(5, "airline", "crisis")

    def test_two_nears(self):
        assert multi_near([(5, "airline", "crisis"), (10, "airline", "climate", "change")]) == (
            "(" + near(5, "airline", "crisis") + "OR " + near(10, "airline", "climate", "change") + ") "
        )

    def test_two_nears_AND(self):
        assert multi_near(
            [(5, "airline", "crisis"), (10, "airline", "climate", "change")],
            method="AND",
        ) == (near(5, "airline", "crisis") + "AND " + near(10, "airline", "climate", "change"))


class TestRepeat:
    """
    Test that `repeat()` generates the correct filters and errors.
    """

    def test_repeat(self):
        assert repeat(3, "environment") == 'repeat3:"environment" '

    def test_repeat_phrase(self):
        with pytest.raises(ValueError, match="single word"):
            repeat(5, "climate change   ")


class TestMultiRepeat:
    """
    Test that `multi_repeat()` generates the correct filters and errors.
    """

    def test_multi_repeat(self):
        assert multi_repeat([(2, "airline"), (3, "airport")], "AND") == ('repeat2:"airline" AND repeat3:"airport" ')

    def test_multi_repeat_or(self):
        assert multi_repeat([(2, "airline"), (3, "airport")], "OR") == ('(repeat2:"airline" OR repeat3:"airport" )')

    def test_multi_repeat_checks_method(self):
        with pytest.raises(ValueError, match="method must be one of AND or OR"):
            multi_repeat([(2, "airline"), (3, "airport")], "NOT_A_METHOD")


class TestTimespan:
    """
    Test that `Filter._validate_timespan` validates timespans correctly
    """

    def test_allows_valid_units(self):
        for unit in VALID_TIMESPAN_UNITS:
            Filters._validate_timespan(f"60{unit}")

    def test_forbids_invalid_units(self):
        with pytest.raises(ValueError, match="is not a supported unit"):
            Filters._validate_timespan("60milliseconds")

    def test_forbids_invalid_values(self):
        invalid_timespans = ["12.5min", "40days0", "2/3weeks"]
        for timespan in invalid_timespans:
            with pytest.raises(ValueError):
                Filters._validate_timespan(timespan)

    def test_forbids_incorrectly_formatted_timespans(self):
        with pytest.raises(ValueError, match="is not a supported unit"):
            Filters._validate_timespan("min15")

    def test_timespan_greater_than_60_mins(self):
        with pytest.raises(ValueError, match="Period must be at least 60 minutes"):
            Filters._validate_timespan("15min")


class TestToneFilterToString:
    def test_single_tone(self):
        assert Filters._tone_to_string("tone", ">5") == "tone>5 "

    def test_rejects_multiple_tones(self):
        with pytest.raises(NotImplementedError, match="Multiple tone values are not supported yet"):
            Filters._tone_to_string("tone", [">5", "<10"])
