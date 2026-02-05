from datetime import datetime, timedelta
from unittest import mock

import pandas as pd
import pytest
from aiohttp import ClientSession
from requests import Response, Session

from gdelt_client import Filters, GdeltClient
from gdelt_client.errors import RateLimitError


class TestArticleSearchAsync:
    """
    Test that the API client behaves correctly when doing an article search query
    """

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_articles_is_a_df(self):
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        f = Filters(keyword="environment", start_date=start_date, end_date=end_date)
        async with GdeltClient() as client:
            articles = await client.aarticle_search(f)

        assert type(articles) is pd.DataFrame

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_correct_columns(self):
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        f = Filters(keyword="environment", start_date=start_date, end_date=end_date)
        async with GdeltClient() as client:
            articles = await client.aarticle_search(f)

        assert list(articles.columns) == [
            "url",
            "url_mobile",
            "title",
            "seendate",
            "socialimage",
            "domain",
            "language",
            "sourcecountry",
        ]

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_rows_returned(self):
        # This test could fail if there really are no articles
        # that match the filter, but given the query used for
        # testing that's very unlikely.
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        f = Filters(keyword="environment", start_date=start_date, end_date=end_date)
        async with GdeltClient() as client:
            articles = await client.aarticle_search(f)

        assert articles.shape[0] >= 1


class TestTimelineSearchAsync:
    """
    Test that the various modes of timeline search behave correctly.
    """

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_all_modes_return_a_df(self):
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        f = Filters(keyword="environment", start_date=start_date, end_date=end_date)

        async with GdeltClient() as gd:
            all_results = []
            for mode in [
                "timelinevol",
                "timelinevolraw",
                "timelinelang",
                "timelinetone",
                "timelinesourcecountry",
            ]:
                result = await gd.atimeline_search(mode, f)
                all_results.append(result)

        assert all(type(result) is pd.DataFrame for result in all_results)

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_all_modes_return_data(self):
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        f = Filters(keyword="environment", start_date=start_date, end_date=end_date)

        async with GdeltClient() as gd:
            all_results = []
            for mode in [
                "timelinevol",
                "timelinevolraw",
                "timelinelang",
                "timelinetone",
                "timelinesourcecountry",
            ]:
                result = await gd.atimeline_search(mode, f)
                all_results.append(result)

        assert all(result.shape[0] >= 1 for result in all_results)

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_unsupported_mode(self):
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        with pytest.raises(ValueError, match="Invalid"):
            async with GdeltClient() as gd:
                await gd.atimeline_search(
                    "unsupported",
                    Filters(
                        keyword="environment",
                        start_date=start_date,
                        end_date=end_date,
                    ),
                )

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_vol_has_two_columns(self):
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        f = Filters(keyword="environment", start_date=start_date, end_date=end_date)

        async with GdeltClient() as gd:
            result = await gd.atimeline_search("timelinevol", f)

        assert result.shape[1] == 2

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_vol_raw_has_three_columns(self):
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        f = Filters(keyword="environment", start_date=start_date, end_date=end_date)

        async with GdeltClient() as gd:
            result = await gd.atimeline_search("timelinevolraw", f)

        assert result.shape[1] == 3

    @pytest.mark.asyncio
    async def test_handles_empty_API_response(self):
        async with GdeltClient() as gd:
            with mock.patch.object(gd, "_aquery", new_callable=mock.AsyncMock) as query_mock:
                query_mock.return_value = {}
                result = await gd.atimeline_search("timelinetone", Filters(keyword="environment", timespan="1h"))
                assert type(result) is pd.DataFrame
                assert result.shape[0] == 0


class TestQueryAsync:
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_handles_invalid_query_string(self):
        async with GdeltClient() as gd:
            with pytest.raises(ValueError, match=r"Invalid query"):
                await gd._aquery("artlist", "environment&timespan=mins15")

    @pytest.mark.asyncio
    async def test_raises_an_error_when_response_is_bad_status_code(self):
        async with ClientSession() as session:
            gd = GdeltClient(aio_session=session)

            mock_response = mock.AsyncMock()
            mock_response.status = 429
            mock_response.reason = "Too Many Requests"
            mock_response.headers = {"content-type": "application/json"}

            with mock.patch.object(session, "get", new_callable=mock.AsyncMock) as mock_get:
                mock_get.return_value = mock_response
                with pytest.raises(RateLimitError):
                    await gd._aquery("artlist", "")


class TestArticleSearchSync:
    """
    Test that the API client behaves correctly when doing an article search query (sync)
    """

    @pytest.mark.integration
    def test_articles_is_a_df(self):
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        f = Filters(keyword="environment", start_date=start_date, end_date=end_date)
        client = GdeltClient()
        articles = client.article_search(f)

        assert type(articles) is pd.DataFrame

    @pytest.mark.integration
    def test_correct_columns(self):
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        f = Filters(keyword="environment", start_date=start_date, end_date=end_date)
        client = GdeltClient()
        articles = client.article_search(f)

        assert list(articles.columns) == [
            "url",
            "url_mobile",
            "title",
            "seendate",
            "socialimage",
            "domain",
            "language",
            "sourcecountry",
        ]

    @pytest.mark.integration
    def test_rows_returned(self):
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        f = Filters(keyword="environment", start_date=start_date, end_date=end_date)
        client = GdeltClient()
        articles = client.article_search(f)

        assert articles.shape[0] >= 1


class TestTimelineSearchSync:
    """
    Test that the various modes of timeline search behave correctly (sync)
    """

    @pytest.mark.integration
    def test_all_modes_return_a_df(self):
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        f = Filters(keyword="environment", start_date=start_date, end_date=end_date)
        gd = GdeltClient()
        all_results = []
        for mode in [
            "timelinevol",
            "timelinevolraw",
            "timelinelang",
            "timelinetone",
            "timelinesourcecountry",
        ]:
            result = gd.timeline_search(mode, f)
            all_results.append(result)

        assert all(type(result) is pd.DataFrame for result in all_results)

    @pytest.mark.integration
    def test_all_modes_return_data(self):
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        f = Filters(keyword="environment", start_date=start_date, end_date=end_date)
        gd = GdeltClient()
        all_results = []
        for mode in [
            "timelinevol",
            "timelinevolraw",
            "timelinelang",
            "timelinetone",
            "timelinesourcecountry",
        ]:
            result = gd.timeline_search(mode, f)
            all_results.append(result)

        assert all(result.shape[0] >= 1 for result in all_results)

    @pytest.mark.integration
    def test_unsupported_mode(self):
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        with pytest.raises(ValueError, match="Invalid"):
            gd = GdeltClient()
            gd.timeline_search(
                "unsupported",
                Filters(
                    keyword="environment",
                    start_date=start_date,
                    end_date=end_date,
                ),
            )

    @pytest.mark.integration
    def test_vol_has_two_columns(self):
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        f = Filters(keyword="environment", start_date=start_date, end_date=end_date)
        gd = GdeltClient()
        result = gd.timeline_search("timelinevol", f)

        assert result.shape[1] == 2

    @pytest.mark.integration
    def test_vol_raw_has_three_columns(self):
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        f = Filters(keyword="environment", start_date=start_date, end_date=end_date)
        gd = GdeltClient()
        result = gd.timeline_search("timelinevolraw", f)

        assert result.shape[1] == 3

    def test_handles_empty_API_response(self):
        gd = GdeltClient()

        with mock.patch.object(gd, "_query") as query_mock:
            query_mock.return_value = {}
            result = gd.timeline_search("timelinetone", Filters(keyword="environment", timespan="1h"))
            assert type(result) is pd.DataFrame
            assert result.shape[0] == 0


class TestQuerySync:
    @pytest.mark.integration
    def test_handles_invalid_query_string(self):
        gd = GdeltClient()

        with pytest.raises(ValueError, match=r"Invalid query"):
            gd._query("artlist", "environment&timespan=mins15")

    def test_raises_an_error_when_response_is_bad_status_code(self):
        gd = GdeltClient(session=Session())

        mock_response = mock.Mock(spec=Response)
        mock_response.status_code = 429
        mock_response.reason = "Too Many Requests"
        mock_response.headers = {"content-type": "application/json"}

        with mock.patch.object(gd.session, "get") as mock_get:
            mock_get.return_value = mock_response
            with pytest.raises(RateLimitError):
                gd._query("artlist", "")


class TestBuildUrls:
    def test_builds_events_url(self):
        from gdelt_client.enums import GdeltTable

        client = GdeltClient()
        urls = client._build_urls(["20200115234500"], GdeltTable.EVENTS, translation=False)

        assert len(urls) == 1
        assert urls[0] == "http://data.gdeltproject.org/gdeltv2/20200115234500.export.CSV.zip"

    def test_builds_mentions_url(self):
        from gdelt_client.enums import GdeltTable

        client = GdeltClient()
        urls = client._build_urls(["20200115234500"], GdeltTable.MENTIONS, translation=False)

        assert urls[0] == "http://data.gdeltproject.org/gdeltv2/20200115234500.mentions.CSV.zip"

    def test_builds_gkg_url(self):
        from gdelt_client.enums import GdeltTable

        client = GdeltClient()
        urls = client._build_urls(["20200115234500"], GdeltTable.GKG, translation=False)

        assert urls[0] == "http://data.gdeltproject.org/gdeltv2/20200115234500.gkg.csv.zip"

    def test_builds_translation_url(self):
        from gdelt_client.enums import GdeltTable

        client = GdeltClient()
        urls = client._build_urls(["20200115234500"], GdeltTable.EVENTS, translation=True)

        assert urls[0] == "http://data.gdeltproject.org/gdeltv2/20200115234500.translation.export.CSV.zip"

    def test_builds_multiple_urls(self):
        from gdelt_client.enums import GdeltTable

        client = GdeltClient()
        urls = client._build_urls(
            ["20200115234500", "20200116234500"],
            GdeltTable.EVENTS,
            translation=False,
        )

        assert len(urls) == 2


class TestSchema:
    def test_returns_dataframe(self):
        client = GdeltClient()
        schema = client.schema("events")

        assert isinstance(schema, pd.DataFrame)

    def test_has_name_column(self):
        client = GdeltClient()
        schema = client.schema("events")

        assert "name" in schema.columns

    def test_works_with_enum(self):
        from gdelt_client.enums import GdeltTable

        client = GdeltClient()
        schema = client.schema(GdeltTable.MENTIONS)

        assert isinstance(schema, pd.DataFrame)

    def test_raises_for_invalid_table(self):
        client = GdeltClient()

        with pytest.raises(ValueError, match="Unknown table"):
            client.schema("invalid")


class TestFormatOutput:
    def test_dataframe_output(self):
        from gdelt_client.enums import OutputFormat

        client = GdeltClient()
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        result = client._format_output(df, OutputFormat.DATAFRAME, normalize_columns=False)

        assert isinstance(result, pd.DataFrame)

    def test_json_output(self):
        from gdelt_client.enums import OutputFormat

        client = GdeltClient()
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        result = client._format_output(df, OutputFormat.JSON, normalize_columns=False)

        assert isinstance(result, str)
        assert "col1" in result

    def test_csv_output(self):
        from gdelt_client.enums import OutputFormat

        client = GdeltClient()
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        result = client._format_output(df, OutputFormat.CSV, normalize_columns=False)

        assert isinstance(result, str)
        assert "col1,col2" in result

    def test_normalize_columns(self):
        from gdelt_client.enums import OutputFormat

        client = GdeltClient()
        df = pd.DataFrame({"Col_One": [1], "Col_Two": [2]})
        result = client._format_output(df, OutputFormat.DATAFRAME, normalize_columns=True)

        assert "colone" in result.columns
        assert "coltwo" in result.columns


class TestCameoCodes:
    def test_lazy_loads_cameo_codes(self):
        client = GdeltClient()

        assert client._cameo_codes is None

        codes = client.cameo_codes

        assert codes is not None
        assert isinstance(codes, pd.DataFrame)
        assert client._cameo_codes is not None

    def test_caches_cameo_codes(self):
        client = GdeltClient()

        codes1 = client.cameo_codes
        codes2 = client.cameo_codes

        assert codes1 is codes2


class TestAddCameoDescriptions:
    def test_adds_description_column(self):
        client = GdeltClient()
        df = pd.DataFrame({"EventCode": ["01", "02"], "OtherCol": [1, 2]})

        result = client._add_cameo_descriptions(df)

        assert "CAMEOCodeDescription" in result.columns

    def test_inserts_after_event_code(self):
        client = GdeltClient()
        df = pd.DataFrame({"Col1": [1], "EventCode": ["01"], "Col2": [2]})

        result = client._add_cameo_descriptions(df)
        cols = list(result.columns)

        assert cols.index("CAMEOCodeDescription") == cols.index("EventCode") + 1

    def test_returns_unchanged_if_no_event_code(self):
        client = GdeltClient()
        df = pd.DataFrame({"Col1": [1], "Col2": [2]})

        result = client._add_cameo_descriptions(df)

        assert "CAMEOCodeDescription" not in result.columns


class TestSearchValidation:
    def test_raises_for_future_date(self):
        client = GdeltClient()
        future = (datetime.today() + timedelta(days=10)).strftime("%Y-%m-%d")

        with pytest.raises(ValueError, match="in the future"):
            client.search(future)

    def test_raises_for_date_before_gdelt_v2(self):
        client = GdeltClient()

        with pytest.raises(ValueError, match=r"before GDELT 2\.0"):
            client.search("2010-01-01")

    def test_raises_for_invalid_table(self):
        client = GdeltClient()

        with pytest.raises(ValueError):
            client.search("2020-01-15", table="invalid")


class TestSearchWithMockedDownload:
    def test_search_returns_dataframe(self):
        client = GdeltClient()
        mock_df = pd.DataFrame({"GLOBALEVENTID": [1, 2], "EventCode": ["01", "02"]})

        with mock.patch.object(client, "_download_and_parse", return_value=mock_df):
            result = client.search("2020-01-15")

        assert isinstance(result, pd.DataFrame)

    def test_search_adds_cameo_descriptions_for_events(self):
        client = GdeltClient()
        mock_df = pd.DataFrame({"GLOBALEVENTID": [1], "EventCode": ["01"]})

        with mock.patch.object(client, "_download_and_parse", return_value=mock_df):
            result = client.search("2020-01-15", table="events")

        assert "CAMEOCodeDescription" in result.columns

    def test_search_returns_json_output(self):
        from gdelt_client.enums import OutputFormat

        client = GdeltClient()
        mock_df = pd.DataFrame({"Col1": [1, 2]})

        with (
            mock.patch.object(client, "_download_and_parse", return_value=mock_df),
            mock.patch.object(client, "_add_cameo_descriptions", return_value=mock_df),
        ):
            result = client.search("2020-01-15", output=OutputFormat.JSON)

        assert isinstance(result, str)

    def test_search_raises_when_no_data(self):
        client = GdeltClient()

        with (
            mock.patch.object(client, "_download_and_parse", return_value=pd.DataFrame()),
            pytest.raises(ValueError, match="No data returned"),
        ):
            client.search("2020-01-15")

    def test_search_with_coverage_calls_multiple_urls(self):
        client = GdeltClient()
        mock_df = pd.DataFrame({"Col1": [1]})

        with (
            mock.patch.object(client, "_download_and_parse", return_value=mock_df) as mock_download,
            mock.patch.object(client, "_add_cameo_descriptions", return_value=mock_df),
        ):
            client.search("2020-01-15", coverage=True)

        assert mock_download.call_count == 96


class TestAsyncSearchWithMockedDownload:
    @pytest.mark.asyncio
    async def test_asearch_returns_dataframe(self):
        client = GdeltClient()
        mock_df = pd.DataFrame({"GLOBALEVENTID": [1, 2], "EventCode": ["01", "02"]})

        with mock.patch.object(client, "_adownload_and_parse", new_callable=mock.AsyncMock, return_value=mock_df):
            result = await client.asearch("2020-01-15")

        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_asearch_raises_when_no_data(self):
        client = GdeltClient()

        with (
            mock.patch.object(client, "_adownload_and_parse", new_callable=mock.AsyncMock, return_value=pd.DataFrame()),
            pytest.raises(ValueError, match="No data returned"),
        ):
            await client.asearch("2020-01-15")

    @pytest.mark.asyncio
    async def test_asearch_with_multiple_urls(self):
        client = GdeltClient()
        mock_df = pd.DataFrame({"Col1": [1]})

        with (
            mock.patch.object(client, "_adownload_and_parse", new_callable=mock.AsyncMock, return_value=mock_df),
            mock.patch.object(client, "_add_cameo_descriptions", return_value=mock_df),
        ):
            result = await client.asearch(["2020-01-15", "2020-01-16"])

        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_asearch_filters_exceptions_from_gather(self):
        client = GdeltClient()
        mock_df = pd.DataFrame({"Col1": [1]})

        async def mock_download(url, table, columns):
            if "20200115" in url:
                raise ValueError("Download failed")
            return mock_df

        with (
            mock.patch.object(client, "_adownload_and_parse", side_effect=mock_download),
            mock.patch.object(client, "_add_cameo_descriptions", return_value=mock_df),
        ):
            result = await client.asearch(["2020-01-15", "2020-01-16"])

        assert isinstance(result, pd.DataFrame)


class TestParseArticles:
    def test_parses_articles_with_data(self):
        from gdelt_client.api_client import _parse_articles

        articles = {
            "articles": [
                {"url": "http://example.com", "title": "Test"},
                {"url": "http://example2.com", "title": "Test2"},
            ]
        }
        result = _parse_articles(articles)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "url" in result.columns

    def test_returns_empty_df_when_no_articles_key(self):
        from gdelt_client.api_client import _parse_articles

        result = _parse_articles({})

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_returns_empty_df_for_empty_articles(self):
        from gdelt_client.api_client import _parse_articles

        result = _parse_articles({"articles": []})

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


class TestParseTimeline:
    def test_parses_timeline_data(self):
        from gdelt_client.api_client import _parse_timeline

        timeline = {
            "timeline": [
                {
                    "series": "Series1",
                    "data": [
                        {"date": "2020-01-15T00:00:00Z", "value": 10},
                        {"date": "2020-01-16T00:00:00Z", "value": 20},
                    ],
                }
            ]
        }
        result = _parse_timeline(timeline, "timelinevol")

        assert isinstance(result, pd.DataFrame)
        assert "datetime" in result.columns
        assert "Series1" in result.columns
        assert len(result) == 2

    def test_parses_volume_raw_with_norm(self):
        from gdelt_client.api_client import _parse_timeline
        from gdelt_client.enums import TimeSeriesMode

        timeline = {
            "timeline": [
                {
                    "series": "Series1",
                    "data": [
                        {"date": "2020-01-15T00:00:00Z", "value": 10, "norm": 100},
                    ],
                }
            ]
        }
        result = _parse_timeline(timeline, TimeSeriesMode.VOLUME_RAW)

        assert "All Articles" in result.columns

    def test_returns_empty_df_for_empty_timeline(self):
        from gdelt_client.api_client import _parse_timeline

        result = _parse_timeline({"timeline": []}, "timelinevol")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_handles_multiple_series(self):
        from gdelt_client.api_client import _parse_timeline

        timeline = {
            "timeline": [
                {
                    "series": "English",
                    "data": [{"date": "2020-01-15T00:00:00Z", "value": 10}],
                },
                {
                    "series": "Spanish",
                    "data": [{"date": "2020-01-15T00:00:00Z", "value": 5}],
                },
            ]
        }
        result = _parse_timeline(timeline, "timelinelang")

        assert "English" in result.columns
        assert "Spanish" in result.columns


class TestArticleSearchMocked:
    def test_article_search_calls_query_and_parses(self):
        client = GdeltClient()
        mock_response = {"articles": [{"url": "http://example.com", "title": "Test"}]}

        with mock.patch.object(client, "_query", return_value=mock_response):
            result = client.article_search(Filters(keyword="test", timespan="1h"))

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1


class TestTimelineSearchMocked:
    def test_timeline_search_calls_query_and_parses(self):
        client = GdeltClient()
        mock_response = {
            "timeline": [
                {
                    "series": "Test",
                    "data": [{"date": "2020-01-15T00:00:00Z", "value": 10}],
                }
            ]
        }

        with mock.patch.object(client, "_query", return_value=mock_response):
            result = client.timeline_search("timelinevol", Filters(keyword="test", timespan="1h"))

        assert isinstance(result, pd.DataFrame)
        assert "Test" in result.columns


class TestQuerySessionCreation:
    def test_creates_session_if_none(self):
        client = GdeltClient()
        assert client.session is None

        mock_response = mock.Mock(spec=Response)
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "application/json"}
        mock_response.content = b'{"test": "data"}'

        with mock.patch("gdelt_client.api_client.Session") as mock_session_class:
            mock_session = mock.Mock()
            mock_session.get.return_value = mock_response
            mock_session_class.return_value = mock_session

            client._query("artlist", "test")

            mock_session_class.assert_called_once()
            mock_session.headers.update.assert_called_once()

    def test_raises_on_html_error_response(self):
        client = GdeltClient()

        mock_response = mock.Mock(spec=Response)
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "text/html"}
        mock_response.text = "Error: Invalid query"

        with (
            mock.patch("gdelt_client.api_client.Session") as mock_session_class,
            pytest.raises(ValueError, match="Invalid query"),
        ):
            mock_session = mock.Mock()
            mock_session.get.return_value = mock_response
            mock_session_class.return_value = mock_session

            client._query("artlist", "test")


class TestAsyncQueryMocked:
    @pytest.mark.asyncio
    async def test_aarticle_search_calls_aquery_and_parses(self):
        client = GdeltClient()
        mock_response = {"articles": [{"url": "http://example.com", "title": "Test"}]}

        with mock.patch.object(client, "_aquery", new_callable=mock.AsyncMock, return_value=mock_response):
            result = await client.aarticle_search(Filters(keyword="test", timespan="1h"))

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_atimeline_search_calls_aquery_and_parses(self):
        client = GdeltClient()
        mock_response = {
            "timeline": [
                {
                    "series": "Test",
                    "data": [{"date": "2020-01-15T00:00:00Z", "value": 10}],
                }
            ]
        }

        with mock.patch.object(client, "_aquery", new_callable=mock.AsyncMock, return_value=mock_response):
            result = await client.atimeline_search("timelinevol", Filters(keyword="test", timespan="1h"))

        assert isinstance(result, pd.DataFrame)


class TestParseGdeltFile:
    def test_parses_zip_file_with_matching_columns(self):
        import io
        import zipfile

        client = GdeltClient()
        columns = ["Col1", "Col2", "Col3"]

        csv_content = "val1\tval2\tval3\n"
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("data.csv", csv_content)
        buffer.seek(0)

        from gdelt_client.enums import GdeltTable

        result = client._parse_gdelt_file(buffer.read(), GdeltTable.MENTIONS, columns)

        assert list(result.columns) == columns

    def test_parses_zip_file_with_one_less_column(self):
        import io
        import zipfile

        client = GdeltClient()
        columns = ["Col1", "Col2", "Col3", "Col4"]

        csv_content = "val1\tval2\tval3\n"
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("data.csv", csv_content)
        buffer.seek(0)

        from gdelt_client.enums import GdeltTable

        result = client._parse_gdelt_file(buffer.read(), GdeltTable.MENTIONS, columns)

        assert list(result.columns) == ["Col1", "Col2", "Col3"]

    def test_warns_on_column_mismatch(self):
        import io
        import zipfile

        client = GdeltClient()
        columns = ["Col1", "Col2"]

        csv_content = "val1\tval2\tval3\tval4\tval5\n"
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("data.csv", csv_content)
        buffer.seek(0)

        from gdelt_client.enums import GdeltTable

        with pytest.warns(UserWarning, match="Column count mismatch"):
            client._parse_gdelt_file(buffer.read(), GdeltTable.MENTIONS, columns)

    def test_uses_dtype_overrides_for_events(self):
        import io
        import zipfile

        client = GdeltClient()
        columns = ["C" + str(i) for i in range(30)]

        csv_content = "\t".join(["v" + str(i) for i in range(30)]) + "\n"
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("data.csv", csv_content)
        buffer.seek(0)

        from gdelt_client.enums import GdeltTable

        result = client._parse_gdelt_file(buffer.read(), GdeltTable.EVENTS, columns)

        assert isinstance(result, pd.DataFrame)


class TestToGeoDataFrame:
    def test_converts_to_geodataframe(self):
        client = GdeltClient()
        df = pd.DataFrame({
            "ActionGeo_Lat": [40.7128, 34.0522],
            "ActionGeo_Long": [-74.0060, -118.2437],
            "Event": ["A", "B"],
        })

        result = client._to_geodataframe(df)

        import geopandas as gpd_module

        assert isinstance(result, gpd_module.GeoDataFrame)
        assert "geometry" in result.columns

    def test_uses_normalized_column_names(self):
        client = GdeltClient()
        df = pd.DataFrame({
            "actiongeolat": [40.7128],
            "actiongeolong": [-74.0060],
            "Event": ["A"],
        })

        result = client._to_geodataframe(df)

        import geopandas as gpd_module

        assert isinstance(result, gpd_module.GeoDataFrame)

    def test_raises_when_missing_lat_lon_columns(self):
        client = GdeltClient()
        df = pd.DataFrame({"Event": ["A", "B"]})

        with pytest.raises(ValueError, match="must contain latitude"):
            client._to_geodataframe(df)

    def test_filters_null_geometries(self):
        client = GdeltClient()
        df = pd.DataFrame({
            "ActionGeo_Lat": [40.7128, None, 34.0522],
            "ActionGeo_Long": [-74.0060, -100.0, None],
            "Event": ["A", "B", "C"],
        })

        result = client._to_geodataframe(df)

        assert len(result) == 1

    def test_geodataframe_output_format(self):
        from gdelt_client.enums import OutputFormat

        client = GdeltClient()
        df = pd.DataFrame({
            "ActionGeo_Lat": [40.7128],
            "ActionGeo_Long": [-74.0060],
        })

        result = client._format_output(df, OutputFormat.GEODATAFRAME, normalize_columns=False)

        import geopandas as gpd_module

        assert isinstance(result, gpd_module.GeoDataFrame)


class TestSearchMultipleUrls:
    def test_search_concatenates_multiple_dataframes(self):
        client = GdeltClient()
        mock_df1 = pd.DataFrame({"Col1": [1, 2]})
        mock_df2 = pd.DataFrame({"Col1": [3, 4]})

        call_count = [0]

        def mock_download(url, table=None, columns=None):
            call_count[0] += 1
            return mock_df1 if call_count[0] == 1 else mock_df2

        with (
            mock.patch.object(client, "_download_and_parse", side_effect=mock_download),
            mock.patch.object(client, "_add_cameo_descriptions", side_effect=lambda df: df),
        ):
            result = client.search(["2020-01-15", "2020-01-16"])

        assert len(result) == 4

    def test_search_filters_none_results(self):
        client = GdeltClient()
        mock_df = pd.DataFrame({"Col1": [1, 2]})

        call_count = [0]

        def mock_download(url, table=None, columns=None):
            call_count[0] += 1
            return mock_df if call_count[0] == 1 else None

        with (
            mock.patch.object(client, "_download_and_parse", side_effect=mock_download),
            mock.patch.object(client, "_add_cameo_descriptions", side_effect=lambda df: df),
        ):
            result = client.search(["2020-01-15", "2020-01-16"])

        assert len(result) == 2

    def test_search_raises_when_all_downloads_fail(self):
        client = GdeltClient()

        with (
            mock.patch.object(client, "_download_and_parse", return_value=None),
            pytest.raises(ValueError, match="No data returned"),
        ):
            client.search(["2020-01-15", "2020-01-16"])


class TestDefaultMatchCase:
    def test_unknown_output_format_returns_dataframe(self):
        from gdelt_client.enums import OutputFormat

        client = GdeltClient()
        df = pd.DataFrame({"Col1": [1, 2]})

        with mock.patch.object(OutputFormat, "__eq__", return_value=False):
            result = client._format_output(df, OutputFormat.DATAFRAME, normalize_columns=False)

        assert isinstance(result, pd.DataFrame)
