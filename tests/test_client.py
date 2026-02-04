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
        session = ClientSession()
        client = GdeltClient(aio_session=session)
        articles = await client.aarticle_search(f)

        assert type(articles) is pd.DataFrame

        await session.close()

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_correct_columns(self):
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        f = Filters(keyword="environment", start_date=start_date, end_date=end_date)
        session = ClientSession()
        client = GdeltClient(aio_session=session)
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

        await session.close()

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_rows_returned(self):
        # This test could fail if there really are no articles
        # that match the filter, but given the query used for
        # testing that's very unlikely.
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        f = Filters(keyword="environment", start_date=start_date, end_date=end_date)
        session = ClientSession()
        client = GdeltClient(aio_session=session)
        articles = await client.aarticle_search(f)

        assert articles.shape[0] >= 1

        await session.close()


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

        session = ClientSession()
        gd = GdeltClient(aio_session=session)
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

        await session.close()

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_all_modes_return_data(self):
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        f = Filters(keyword="environment", start_date=start_date, end_date=end_date)

        session = ClientSession()
        gd = GdeltClient(aio_session=session)
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

        await session.close()

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_unsupported_mode(self):
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        with pytest.raises(ValueError, match="not in supported API modes"):
            session = ClientSession()
            gd = GdeltClient(aio_session=session)
            try:
                await gd.atimeline_search(
                    "unsupported",
                    Filters(
                        keyword="environment",
                        start_date=start_date,
                        end_date=end_date,
                    ),
                )
            finally:
                await session.close()

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_vol_has_two_columns(self):
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        f = Filters(keyword="environment", start_date=start_date, end_date=end_date)

        session = ClientSession()
        gd = GdeltClient(aio_session=session)
        result = await gd.atimeline_search("timelinevol", f)

        assert result.shape[1] == 2

        await session.close()

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_vol_raw_has_three_columns(self):
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=6)).strftime("%Y-%m-%d")

        f = Filters(keyword="environment", start_date=start_date, end_date=end_date)

        session = ClientSession()
        gd = GdeltClient(aio_session=session)
        result = await gd.atimeline_search("timelinevolraw", f)

        assert result.shape[1] == 3

        await session.close()

    @pytest.mark.asyncio
    async def test_handles_empty_API_response(self):
        session = ClientSession()
        gd = GdeltClient(aio_session=session)

        with mock.patch.object(gd, "_aquery", new_callable=mock.AsyncMock) as query_mock:
            query_mock.return_value = {}
            result = await gd.atimeline_search("timelinetone", Filters(keyword="environment", timespan="1h"))
            assert type(result) is pd.DataFrame
            assert result.shape[0] == 0

        await session.close()


class TestQueryAsync:
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_handles_invalid_query_string(self):
        session = ClientSession()
        gd = GdeltClient(aio_session=session)

        with pytest.raises(ValueError, match=r"The query was not valid. The API error message was"):
            await gd._aquery("artlist", "environment&timespan=mins15")

        await session.close()

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_raises_an_error_when_response_is_bad_status_code(self):
        session = ClientSession()
        gd = GdeltClient(aio_session=session)

        mock_response = mock.AsyncMock()
        mock_response.status = 429
        mock_response.reason = "Too Many Requests"
        mock_response.headers = {"content-type": "application/json"}

        with mock.patch.object(session, "get", new_callable=mock.AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            with pytest.raises(RateLimitError):
                await gd._aquery("artlist", "")

        await session.close()


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

        with pytest.raises(ValueError, match="not in supported API modes"):
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

        with pytest.raises(ValueError, match=r"The query was not valid. The API error message was"):
            gd._query("artlist", "environment&timespan=mins15")

    @pytest.mark.integration
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
