import pandas as pd
from aiohttp import ClientSession
from requests import Session

from gdelt_client.enums import ArticleMode, Mode, TimeSeriesMode
from gdelt_client.errors import raise_response_error
from gdelt_client.filters import Filters
from gdelt_client.helpers import load_json


class GdeltClient:
    """
    API client for the GDELT 2.0 API

    ```
    from gdeltdoc import GdeltClient, Filters

    f = Filters(
        keyword = "climate change",
        start_date = "2020-05-10",
        end_date = "2020-05-11"
    )

    gd = GdeltClient()

    # Search for articles matching the filters (requests)
    articles = gd.article_search(f)

    # Get a timeline of the number of articles matching the filters (requests)
    timeline = gd.timeline_search("timelinevol", f)

    # Search for articles matching the filters (aiohttp)
    articles = await gd.aarticle_search(f)

    # Get a timeline of the number of articles matching the filters (aiohttp)
    timeline = await gd.atimeline_search("timelinevol", f)
    ```

    ### Article List
    The article list mode of the API generates a list of news articles that match the filters.
    The client returns this as a pandas DataFrame with columns `url`, `url_mobile`, `title`,
    `seendate`, `socialimage`, `domain`, `language`, `sourcecountry`.

    ### Timeline Search
    There are 5 available modes when making a timeline search:
    * `timelinevol` - a timeline of the volume of news coverage matching the filters,
        represented as a percentage of the total news articles monitored by GDELT.
    * `timelinevolraw` - similar to `timelinevol`, but has the actual number of articles
        and a total rather than a percentage
    * `timelinelang` - similar to `timelinevol` but breaks the total articles down by published language.
        Each language is returned as a separate column in the DataFrame.
    * `timelinesourcecountry` - similar to `timelinevol` but breaks the total articles down by the country
        they were published in. Each country is returned as a separate column in the DataFrame.
    * `timelinetone` - a timeline of the average tone of the news coverage matching the filters.
        See [GDELT's documentation](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/)
        for more information about the tone metric.
    """

    def __init__(
        self,
        json_parsing_max_depth: int = 100,
        session: Session | None = None,
        aio_session: ClientSession | None = None,
    ) -> None:
        """
        Params
        ------
        json_parsing_max_depth
            A parameter for the json parsing function that removes illegal character. If 100 it will remove at max
            100 characters before exiting with an exception
        session
            A `requests.Session` object to use for making API calls. If not provided, a new session will be created.
        aio_session
            An `aiohttp.ClientSession` object to use for making asynchronous API calls.
            If not provided, a new session will be created.
        """
        self.max_depth_json_parsing: int = json_parsing_max_depth
        self.default_headers: dict[str, str] = {
            "User-Agent": "GDELT Python API client - https://github.com/BobMerkus/gdelt-client",
        }
        self.session: Session | None = session
        self.aio_session: ClientSession | None = aio_session

    def article_search(self, filters: Filters) -> pd.DataFrame:
        """
        Make a query against the `ArtList` API to return a DataFrame of news articles that
        match the supplied filters.

        Params
        ------
        filters
            A `gdelt-doc.Filters` object containing the filter parameters for this query.

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame of the articles returned from the API.
        """
        articles = self._query("artlist", filters.query_string)
        return self._parse_articles(articles)

    def timeline_search(self, mode: str, filters: Filters) -> pd.DataFrame:
        """
        Make a query using one of the API's timeline modes.

        Params
        ------
        mode
            The API mode to call. Must be one of "timelinevol", "timelinevolraw",
            "timelinetone", "timelinelang", "timelinesourcecountry".

            See https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/ for a
            longer description of each mode.

        filters
            A `gdelt-doc.Filters` object containing the filter parameters for this query.

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame of the articles returned from the API.
        """
        timeline = self._query(mode, filters.query_string)
        return self._parse_timeline(timeline, mode)

    def _parse_articles(self, articles: dict) -> pd.DataFrame:
        """Parse articles response into DataFrame."""
        if "articles" in articles:
            return pd.DataFrame(articles["articles"])
        return pd.DataFrame()

    def _parse_timeline(self, timeline: dict, mode: str) -> pd.DataFrame:
        """Parse timeline response into DataFrame."""
        if (timeline == {}) or (len(timeline["timeline"]) == 0):
            return pd.DataFrame()

        results = {"datetime": [entry["date"] for entry in timeline["timeline"][0]["data"]]}

        for series in timeline["timeline"]:
            results[series["series"]] = [entry["value"] for entry in series["data"]]

        if mode == "timelinevolraw":
            results["All Articles"] = [entry["norm"] for entry in timeline["timeline"][0]["data"]]

        formatted = pd.DataFrame(results)
        formatted["datetime"] = pd.to_datetime(formatted["datetime"])

        return formatted

    def _query(self, mode: str | Mode, query_string: str) -> dict:
        """
        Submit a query to the GDELT API and return the results as a parsed JSON object.

        Params
        ------
        mode
            The API mode to call. Must be one of "artlist", "timelinevol",
            "timelinevolraw", "timelinetone", "timelinelang", "timelinesourcecountry".

        query_string
            The query parameters and date range to call the API with.

        Returns
        -------
        Dict
            The parsed JSON response from the API.
        """
        if mode not in list(TimeSeriesMode) + list(ArticleMode):
            raise ValueError(f"Mode {mode} not in supported API modes")
        elif isinstance(mode, Mode):
            mode = mode.value
        if self.session is None:
            self.session = Session()
            self.session.headers.update(self.default_headers)
        response = self.session.get(
            f"https://api.gdeltproject.org/api/v2/doc/doc?query={query_string}&mode={mode}&format=json",
        )

        raise_response_error(response=response)

        # Sometimes the API responds to an invalid request with a 200 status code
        # and a text/html content type. I can't figure out a pattern for when that happens so
        # this raises a ValueError with the response content instead of one of the library's
        # custom error types.
        if "text/html" in response.headers["content-type"]:
            raise ValueError(f"The query was not valid. The API error message was: {response.text.strip()}")

        return load_json(response.content, self.max_depth_json_parsing)

    async def aarticle_search(self, filters: Filters) -> pd.DataFrame:
        """
        Make a query against the `ArtList` API to return a DataFrame of news articles that
        match the supplied filters.

        Params
        ------
        filters
            A `gdelt-client.Filters` object containing the filter parameters for this query.

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame of the articles returned from the API.
        """
        articles = await self._aquery(ArticleMode.ARTICLE_LIST, filters.query_string)
        return self._parse_articles(articles)

    async def atimeline_search(self, mode: str | TimeSeriesMode, filters: Filters) -> pd.DataFrame:
        """
        Make a query using one of the API's timeline modes.

        Params
        ------
        mode
            The API mode to call. Must be one of "timelinevol", "timelinevolraw",
            "timelinetone", "timelinelang", "timelinesourcecountry".

            See https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/ for a
            longer description of each mode.

        filters
            A `gdelt-client.Filters` object containing the filter parameters for this query.

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame of the articles returned from the API.
        """
        timeline = await self._aquery(mode, filters.query_string)
        return self._parse_timeline(timeline, mode if isinstance(mode, str) else mode.value)

    async def _aquery(self, mode: str | Mode, query_string: str) -> dict:
        """
        Submit a query to the GDELT API and return the results as a parsed JSON object.

        Params
        ------
        mode
            The API mode to call. Must be one of "artlist", "timelinevol",
            "timelinevolraw", "timelinetone", "timelinelang", "timelinesourcecountry".

        query_string
            The query parameters and date range to call the API with.

        Returns
        -------
        dict
            The parsed JSON response from the API.
        """
        if isinstance(mode, str) and mode not in list(TimeSeriesMode) + list(ArticleMode):
            raise ValueError(f"Mode {mode} not in supported API modes")
        elif isinstance(mode, Mode):
            mode = mode.value
        if self.aio_session is None:
            self.aio_session = ClientSession(headers=self.default_headers)
        response = await self.aio_session.get(
            f"https://api.gdeltproject.org/api/v2/doc/doc?query={query_string}&mode={mode}&format=json"
        )

        raise_response_error(response=response)

        # Sometimes the API responds to an invalid request with a 200 status code
        # and a text/html content type. I can't figure out a pattern for when that happens so
        # this raises a ValueError with the response content instead of one of the library's
        # custom error types.
        if "text/html" in response.headers["content-type"]:
            raise ValueError(f"The query was not valid. The API error message was: {response.text()}")

        try:
            data = await response.read()
            return load_json(data, self.max_depth_json_parsing)
        except ValueError as e:
            raise ValueError("Failed to parse JSON response from API") from e
        finally:
            await response.release()
