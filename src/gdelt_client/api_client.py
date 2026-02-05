from __future__ import annotations

import asyncio
import warnings
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from io import BytesIO
from typing import TYPE_CHECKING

import pandas as pd
from aiohttp import ClientSession
from requests import Session

from gdelt_client.enums import ArticleMode, GdeltTable, Mode, OutputFormat, TimeSeriesMode
from gdelt_client.errors import raise_response_error
from gdelt_client.filters import Filters
from gdelt_client.helpers import (
    Date,
    expand_dates,
    get_cameo_description,
    load_cameo_codes,
    load_json,
    load_schema,
)
from gdelt_client.validation import validate_date, validate_table

if TYPE_CHECKING:
    import geopandas as gpd


def _parse_articles(articles: dict) -> pd.DataFrame:
    """Parse articles response into DataFrame."""
    if "articles" in articles:
        return pd.DataFrame(articles["articles"])
    return pd.DataFrame()


def _parse_timeline(timeline: dict, mode: str | Mode) -> pd.DataFrame:
    """Parse timeline response into DataFrame."""
    if (timeline == {}) or (len(timeline["timeline"]) == 0):
        return pd.DataFrame()

    results: dict[str, list] = {"datetime": [entry["date"] for entry in timeline["timeline"][0]["data"]]}

    for series in timeline["timeline"]:
        results[series["series"]] = [entry["value"] for entry in series["data"]]

    if mode == TimeSeriesMode.VOLUME_RAW:
        results["All Articles"] = [entry["norm"] for entry in timeline["timeline"][0]["data"]]

    formatted = pd.DataFrame(results)
    formatted["datetime"] = pd.to_datetime(formatted["datetime"])

    return formatted


class GdeltClient:
    """
    API client for GDELT 2.0.

    Supports two types of queries:

    1. **DOC API** (article_search, timeline_search):
       Search for news articles and get timeline data via the GDELT DOC API.

    2. **Raw Data Downloads** (search):
       Download and parse raw GDELT event/mention/GKG data files.

    Both synchronous (requests) and asynchronous (aiohttp) interfaces are supported.

    Examples
    --------
    DOC API usage:

    >>> from gdelt_client import GdeltClient, Filters
    >>> client = GdeltClient()
    >>> filters = Filters(keyword="climate change", start_date="2024-01-01", end_date="2024-01-02")
    >>> articles = client.article_search(filters)
    >>> timeline = client.timeline_search("timelinevol", filters)

    Raw data download:

    >>> from gdelt_client import GdeltClient, GdeltTable
    >>> client = GdeltClient()
    >>> events = client.search("2024-01-15", table=GdeltTable.EVENTS)
    >>> mentions = client.search(["2024-01-15", "2024-01-16"], table=GdeltTable.MENTIONS, coverage=True)

    Async usage:

    >>> articles = await client.aarticle_search(filters)
    >>> events = await client.asearch("2024-01-15", table=GdeltTable.EVENTS)
    """

    GDELT_BASE_URL = "http://data.gdeltproject.org/gdeltv2/"
    DOC_API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

    def __init__(
        self,
        json_parsing_max_depth: int = 100,
        session: Session | None = None,
        aio_session: ClientSession | None = None,
        max_workers: int | None = None,
    ) -> None:
        """
        Initialize the GDELT client.

        Parameters
        ----------
        json_parsing_max_depth
            Maximum recursion depth for JSON parsing error recovery.
        session
            Optional requests.Session for synchronous HTTP calls.
        aio_session
            Optional aiohttp.ClientSession for asynchronous HTTP calls.
        max_workers
            Maximum number of parallel workers for downloading files.
            Defaults to None (uses ThreadPoolExecutor default).
        """
        self.max_depth_json_parsing = json_parsing_max_depth
        self.default_headers: dict[str, str] = {
            "User-Agent": "GDELT Python API client - https://github.com/BobMerkus/gdelt-client",
        }
        self.session = session
        self.aio_session = aio_session
        self.max_workers = max_workers
        self._cameo_codes: pd.DataFrame | None = None

    @property
    def cameo_codes(self) -> pd.DataFrame:
        """Lazy-load CAMEO codes lookup table."""
        if self._cameo_codes is None:
            self._cameo_codes = load_cameo_codes()
        return self._cameo_codes

    def article_search(self, filters: Filters) -> pd.DataFrame:
        """
        Search for articles matching the filters using the DOC API.

        Parameters
        ----------
        filters
            A Filters object containing the query parameters.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns: url, url_mobile, title, seendate,
            socialimage, domain, language, sourcecountry.
        """
        articles = self._query(ArticleMode.ARTICLE_LIST, filters.query_string)
        return _parse_articles(articles)

    def timeline_search(self, mode: str | TimeSeriesMode, filters: Filters) -> pd.DataFrame:
        """
        Get timeline data using the DOC API.

        Parameters
        ----------
        mode
            Timeline mode: "timelinevol", "timelinevolraw", "timelinetone",
            "timelinelang", or "timelinesourcecountry".
        filters
            A Filters object containing the query parameters.

        Returns
        -------
        pd.DataFrame
            DataFrame with datetime index and data columns.
        """
        timeline = self._query(mode, filters.query_string)
        return _parse_timeline(timeline, mode)

    def _query(self, mode: str | Mode, query_string: str) -> dict:
        """Execute a DOC API query (sync)."""
        if self.session is None:
            self.session = Session()
            self.session.headers.update(self.default_headers)

        response = self.session.get(
            f"{self.DOC_API_URL}?query={query_string}&mode={mode}&format=json",
        )

        raise_response_error(response=response)

        if "text/html" in response.headers.get("content-type", ""):
            raise ValueError(f"Invalid query. API error: {response.text.strip()}")

        return load_json(response.content, self.max_depth_json_parsing)

    async def aarticle_search(self, filters: Filters) -> pd.DataFrame:
        """Async version of article_search."""
        articles = await self._aquery(ArticleMode.ARTICLE_LIST, filters.query_string)
        return _parse_articles(articles)

    async def atimeline_search(self, mode: str | TimeSeriesMode, filters: Filters) -> pd.DataFrame:
        """Async version of timeline_search."""
        timeline = await self._aquery(mode, filters.query_string)
        return _parse_timeline(timeline, mode)

    async def _aquery(self, mode: str | Mode, query_string: str) -> dict:
        """Execute a DOC API query (async)."""
        if self.aio_session is None:
            self.aio_session = ClientSession(headers=self.default_headers)

        response = await self.aio_session.get(f"{self.DOC_API_URL}?query={query_string}&mode={mode}&format=json")

        raise_response_error(response=response)

        content_type = response.headers.get("content-type", "")
        if "text/html" in content_type:
            text = await response.text()
            raise ValueError(f"Invalid query. API error: {text.strip()}")

        try:
            data = await response.read()
            return load_json(data, self.max_depth_json_parsing)
        except ValueError as e:
            raise ValueError("Failed to parse JSON response from API") from e
        finally:
            response.release()

    def search(
        self,
        date: Date | list[Date],
        table: GdeltTable | str = GdeltTable.EVENTS,
        coverage: bool = False,
        translation: bool = False,
        output: OutputFormat | str = OutputFormat.DATAFRAME,
        normalize_columns: bool = False,
    ) -> pd.DataFrame | str | dict | gpd.GeoDataFrame:
        """
        Download and parse GDELT 2.0 data files.

        Parameters
        ----------
        date
            Date specification:
            - Single date string or datetime
            - Two-element list [start_date, end_date] for a range
            - List of specific dates
        table
            GDELT table: "events", "mentions", or "gkg".
        coverage
            If True, download all 15-minute intervals for each day.
            If False, download only the latest file for each day.
        translation
            If True, download translated data instead of English.
        output
            Output format: "df" (DataFrame), "json", "csv", or "gpd" (GeoDataFrame).
        normalize_columns
            If True, lowercase column names and remove underscores.

        Returns
        -------
        pd.DataFrame | str | dict | gpd.GeoDataFrame
            Data in the requested format.

        Raises
        ------
        ValueError
            If date or table parameters are invalid, or if no data is returned.
        """
        table_enum = GdeltTable(table) if isinstance(table, str) else table
        output_enum = OutputFormat(output) if isinstance(output, str) else output

        validate_date(date)
        validate_table(table_enum, translation)

        date_strings = expand_dates(date, coverage)
        urls = self._build_urls(date_strings, table_enum, translation)
        columns = load_schema(table_enum.value)

        if len(urls) == 1:
            results = self._download_and_parse(urls[0], table_enum, columns)
        else:
            download_fn = partial(self._download_and_parse, table=table_enum, columns=columns)
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                dfs = list(executor.map(download_fn, urls))

            valid_dfs = [df for df in dfs if df is not None and not df.empty]
            if not valid_dfs:
                raise ValueError("No data returned for the specified date(s).")

            results = pd.concat(valid_dfs, ignore_index=True)

        if results is None or results.empty:
            raise ValueError("No data returned for the specified date(s).")

        if table_enum == GdeltTable.EVENTS:
            results = self._add_cameo_descriptions(results)

        return self._format_output(results, output_enum, normalize_columns)

    async def asearch(
        self,
        date: Date | list[Date],
        table: GdeltTable | str = GdeltTable.EVENTS,
        coverage: bool = False,
        translation: bool = False,
        output: OutputFormat | str = OutputFormat.DATAFRAME,
        normalize_columns: bool = False,
    ) -> pd.DataFrame | str | dict | gpd.GeoDataFrame:
        """
        Async version of search().

        Downloads files concurrently using asyncio.gather().
        See search() for parameter documentation.
        """
        table_enum = GdeltTable(table) if isinstance(table, str) else table
        output_enum = OutputFormat(output) if isinstance(output, str) else output

        validate_date(date)
        validate_table(table_enum, translation)

        date_strings = expand_dates(date, coverage)
        urls = self._build_urls(date_strings, table_enum, translation)
        columns = load_schema(table_enum.value)

        if len(urls) == 1:
            results = await self._adownload_and_parse(urls[0], table_enum, columns)
        else:
            tasks = [self._adownload_and_parse(url, table_enum, columns) for url in urls]
            dfs = await asyncio.gather(*tasks, return_exceptions=True)

            valid_dfs = [df for df in dfs if isinstance(df, pd.DataFrame) and not df.empty]
            if not valid_dfs:
                raise ValueError("No data returned for the specified date(s).")

            results = pd.concat(valid_dfs, ignore_index=True)

        if results is None or results.empty:
            raise ValueError("No data returned for the specified date(s).")

        if table_enum == GdeltTable.EVENTS:
            results = self._add_cameo_descriptions(results)

        return self._format_output(results, output_enum, normalize_columns)

    def schema(self, table: GdeltTable | str) -> pd.DataFrame:
        """
        Get the schema for a GDELT table.

        Parameters
        ----------
        table
            Table name: "events", "mentions", or "gkg".

        Returns
        -------
        pd.DataFrame
            DataFrame with column information (name, type, description).
        """
        import json

        from gdelt_client.helpers import SCHEMA_DIR

        table_str = table.value if isinstance(table, GdeltTable) else table
        schema_files = {
            "events": "eventsv2.json",
            "mentions": "mentions.json",
            "gkg": "gkgv2.json",
        }

        if table_str not in schema_files:
            raise ValueError(f"Unknown table: {table_str}")

        schema_path = SCHEMA_DIR / schema_files[table_str]
        with schema_path.open() as f:
            schema_data = json.load(f)

        fields = schema_data["schema"]["fields"]
        return pd.DataFrame(fields)

    def _build_urls(
        self,
        date_strings: list[str],
        table: GdeltTable,
        translation: bool,
    ) -> list[str]:
        """Build download URLs for GDELT data files."""
        suffixes = {
            GdeltTable.EVENTS: ".export.CSV.zip" if not translation else ".translation.export.CSV.zip",
            GdeltTable.MENTIONS: ".mentions.CSV.zip" if not translation else ".translation.mentions.CSV.zip",
            GdeltTable.GKG: ".gkg.csv.zip" if not translation else ".translation.gkg.csv.zip",
        }

        suffix = suffixes[table]
        return [f"{self.GDELT_BASE_URL}{date_str}{suffix}" for date_str in date_strings]

    def _download_and_parse(
        self,
        url: str,
        table: GdeltTable,
        columns: list[str],
    ) -> pd.DataFrame | None:
        """Download and parse a single GDELT data file (sync)."""
        if self.session is None:
            self.session = Session()
            self.session.headers.update(self.default_headers)

        try:
            response = self.session.get(url, timeout=30)

            if response.status_code == 404:
                warnings.warn(f"No data available for URL: {url}", stacklevel=2)
                return None

            response.raise_for_status()
            return self._parse_gdelt_file(response.content, table, columns)

        except Exception as e:
            warnings.warn(f"Failed to download {url}: {e}", stacklevel=2)
            return None

    async def _adownload_and_parse(
        self,
        url: str,
        table: GdeltTable,
        columns: list[str],
    ) -> pd.DataFrame | None:
        """Download and parse a single GDELT data file (async)."""
        if self.aio_session is None:
            self.aio_session = ClientSession(headers=self.default_headers)

        try:
            async with self.aio_session.get(url, timeout=30) as response:
                if response.status == 404:
                    warnings.warn(f"No data available for URL: {url}", stacklevel=2)
                    return None

                response.raise_for_status()
                content = await response.read()
                return self._parse_gdelt_file(content, table, columns)

        except Exception as e:
            warnings.warn(f"Failed to download {url}: {e}", stacklevel=2)
            return None

    def _parse_gdelt_file(
        self,
        data: bytes,
        table: GdeltTable,
        columns: list[str],
    ) -> pd.DataFrame:
        """Parse compressed GDELT CSV/TSV data."""
        buffer = BytesIO(data)

        dtype_overrides: dict[int, str] = {}
        if table == GdeltTable.EVENTS:
            dtype_overrides = {26: "str", 27: "str", 28: "str"}

        df = pd.read_csv(
            buffer,
            compression="zip",
            sep="\t",
            header=None,
            on_bad_lines="skip",
            dtype=dtype_overrides,
            low_memory=False,
        )

        if len(df.columns) == len(columns):
            df.columns = pd.Index(columns)
        elif len(df.columns) == len(columns) - 1:
            df.columns = pd.Index(columns[:-1])
        else:
            warnings.warn(
                f"Column count mismatch: expected {len(columns)}, got {len(df.columns)}",
                stacklevel=2,
            )

        buffer.close()
        return df

    def _add_cameo_descriptions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add human-readable CAMEO code descriptions to events DataFrame."""
        if "EventCode" not in df.columns:
            return df

        codes_df = self.cameo_codes
        descriptions = df["EventCode"].apply(lambda x: get_cameo_description(str(x), codes_df))

        insert_idx = df.columns.get_loc("EventCode") + 1
        df.insert(insert_idx, "CAMEOCodeDescription", descriptions)

        return df

    def _format_output(
        self,
        df: pd.DataFrame,
        output: OutputFormat,
        normalize_columns: bool,
    ) -> pd.DataFrame | str | dict | gpd.GeoDataFrame:
        """Format the output based on the requested format."""
        if normalize_columns:
            df.columns = pd.Index([col.replace("_", "").lower() for col in df.columns])

        match output:
            case OutputFormat.DATAFRAME:
                return df
            case OutputFormat.JSON:
                return df.to_json(orient="records")
            case OutputFormat.CSV:
                return df.to_csv(index=False)
            case OutputFormat.GEODATAFRAME:
                return self._to_geodataframe(df)
            case _:
                return df

    def _to_geodataframe(self, df: pd.DataFrame) -> gpd.GeoDataFrame:
        """Convert DataFrame to GeoDataFrame with geometry column."""
        try:
            import geopandas as gpd_module
            from shapely.geometry import Point
        except ImportError as e:
            raise ImportError(
                "geopandas and shapely are required for GeoDataFrame output. "
                "Install with: pip install geopandas shapely"
            ) from e

        lat_col = "ActionGeo_Lat" if "ActionGeo_Lat" in df.columns else "actiongeolat"
        lon_col = "ActionGeo_Long" if "ActionGeo_Long" in df.columns else "actiongeolong"

        if lat_col not in df.columns or lon_col not in df.columns:
            raise ValueError(
                f"DataFrame must contain latitude ({lat_col}) and longitude ({lon_col}) columns "
                "for GeoDataFrame conversion."
            )

        filtered = df[df[lat_col].notna() & df[lon_col].notna()].copy()

        geometry = [
            Point(lon, lat) if pd.notna(lon) and pd.notna(lat) else None
            for lon, lat in zip(filtered[lon_col], filtered[lat_col], strict=False)
        ]

        gdf = gpd_module.GeoDataFrame(filtered, geometry=geometry, crs="EPSG:4326")
        gdf.columns = pd.Index([col.replace("_", "").lower() for col in gdf.columns])

        return gdf[gdf.geometry.notna()]
