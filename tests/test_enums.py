from gdelt_client.enums import (
    ArticleMode,
    GdeltTable,
    HttpResponseCodes,
    OutputFormat,
    TimeSeriesMode,
)


class TestGdeltTable:
    def test_has_expected_values(self):
        assert GdeltTable.EVENTS == "events"
        assert GdeltTable.MENTIONS == "mentions"
        assert GdeltTable.GKG == "gkg"

    def test_is_string_enum(self):
        assert str(GdeltTable.EVENTS) == "events"


class TestOutputFormat:
    def test_has_expected_values(self):
        assert OutputFormat.DATAFRAME == "df"
        assert OutputFormat.JSON == "json"
        assert OutputFormat.CSV == "csv"
        assert OutputFormat.GEODATAFRAME == "gpd"


class TestTimeSeriesMode:
    def test_has_expected_values(self):
        assert TimeSeriesMode.VOLUME == "timelinevol"
        assert TimeSeriesMode.VOLUME_RAW == "timelinevolraw"
        assert TimeSeriesMode.TONE == "timelinetone"
        assert TimeSeriesMode.LANGUAGE == "timelinelang"
        assert TimeSeriesMode.SOURCE_COUNTRY == "timelinesourcecountry"


class TestArticleMode:
    def test_has_expected_values(self):
        assert ArticleMode.ARTICLE_LIST == "artlist"


class TestHttpResponseCodes:
    def test_has_expected_values(self):
        assert HttpResponseCodes.OK.value == 200
        assert HttpResponseCodes.BAD_REQUEST.value == 400
        assert HttpResponseCodes.NOT_FOUND.value == 404
        assert HttpResponseCodes.RATE_LIMIT.value == 429
