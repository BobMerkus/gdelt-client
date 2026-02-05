# GDELT 2.0 API Client

A Python client to fetch data from the [GDELT 2.0 API](https://gdeltproject.org/).

This client supports both the DOC API for article search and timelines, as well as direct access to GDELT's raw event data files (events, mentions, and GKG). This allows for simpler, small-scale analysis of news coverage and events data without having to deal with the complexities of downloading and managing the raw files from S3, or working with the BigQuery export.

The implementation has been forked from [gdeltdoc](https://github.com/alex9smith/gdelt-doc-api).

## Installation

`gdelt-client` is on [PyPi](https://pypi.org/project/gdelt-client/) and is installed through pip:

```bash
pip install gdelt-client
```

## Use

### DOC API - Article Search & Timelines

Search for news articles and get timeline data via the GDELT DOC API.

```python
from gdelt_client import GdeltClient, Filters

f = Filters(
    keyword="climate change",
    start_date="2020-05-10",
    end_date="2020-05-11"
)

gd = GdeltClient()

# Search for articles matching the filters
articles = gd.article_search(f)

# Get a timeline of coverage volume
timeline = gd.timeline_search("timelinevol", f)
```

**Async example:**

```python
import asyncio
from gdelt_client import GdeltClient, Filters

async def main():
    f = Filters(keyword="climate change", start_date="2020-05-10", end_date="2020-05-11")

    # Use async context manager to properly cleanup resources
    async with GdeltClient() as gd:
        # Async article search
        articles = await gd.aarticle_search(f)

        # Async timeline search
        timeline = await gd.atimeline_search("timelinevol", f)

asyncio.run(main())
```

### Raw Data Downloads - Events, Mentions & GKG

Download and parse GDELT's raw data files directly. Returns data with CAMEO code descriptions for events.

```python
from gdelt_client import GdeltClient, GdeltTable, OutputFormat

gd = GdeltClient()

# Download events for a single date
events = gd.search(
    date="2020-05-10",
    table=GdeltTable.EVENTS,
    output=OutputFormat.DATAFRAME
)

# Download mentions for a date range with full 15-min coverage
mentions = gd.search(
    date=["2020-05-10", "2020-05-11"],
    table=GdeltTable.MENTIONS,
    coverage=True  # Download all 15-minute intervals
)

# Get GeoDataFrame with geometry for mapping
geo_events = gd.search(
    date="2020-05-10",
    table=GdeltTable.EVENTS,
    output=OutputFormat.GEODATAFRAME
)

# View table schema
schema = gd.schema(GdeltTable.EVENTS)
```

**Async example** (downloads files concurrently for better performance):

```python
import asyncio
from gdelt_client import GdeltClient, GdeltTable

async def main():
    # Use async context manager to properly cleanup resources
    async with GdeltClient() as gd:
        # Async search with concurrent file downloads
        events = await gd.asearch(
            date=["2020-05-10", "2020-05-11"],
            table=GdeltTable.EVENTS,
            coverage=True
        )
    print(events[:5])
    print(f"Total records {len(events)}")
asyncio.run(main())
```

**Available tables:** `EVENTS`, `MENTIONS`, `GKG`  
**Available output formats:** `DATAFRAME`, `JSON`, `CSV`, `GEODATAFRAME`

### Article List

The `article_search()` method (and async `aarticle_search()`) generates a list of news articles that match the filters. Returns a pandas DataFrame with columns: `url`, `url_mobile`, `title`, `seendate`, `socialimage`, `domain`, `language`, `sourcecountry`.

### Timeline Search

The `timeline_search()` method (and async `atimeline_search()`) supports 5 modes:

- `timelinevol` - Timeline of coverage volume as a percentage of all monitored articles
- `timelinevolraw` - Timeline with actual article counts instead of percentages
- `timelinelang` - Coverage broken down by language (each language as a column)
- `timelinesourcecountry` - Coverage broken down by source country (each country as a column)
- `timelinetone` - Average tone of articles over time (see [GDELT docs](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/) for tone metric details)

All modes return a pandas DataFrame with a `datetime` column and data columns.

### Filters

The search query passed to the API is constructed from a `gdelt_client.Filters` object.

```python
from gdelt_client import Filters, near, repeat

f = Filters(
    start_date = "2020-05-01",
    end_date = "2020-05-02",
    num_records = 250,
    keyword = "climate change",
    domain = ["bbc.co.uk", "nytimes.com"],
    country = ["UK", "US"],
    theme = "GENERAL_HEALTH",
    near = near(10, "airline", "carbon"),
    repeat = repeat(5, "planet")
)
```

Filters for `keyword`, `domain`, `domain_exact`, `country`, `language` and `theme` can be passed either as a single string or as a list of strings. If a list is passed, the values in the list are wrappeed in a boolean OR.

You must pass either `start_date` and `end_date`, or `timespan`

- `start_date` - The start date for the filter in YYYY-MM-DD format or as a datetime object in UTC time.
  Passing a datetime allows you to specify a time down to seconds granularity. The API officially only supports the most recent 3 months of articles. Making a request for an earlier date range may still return data, but it's not guaranteed.
- `end_date` - The end date for the filter in YYYY-MM-DD format or as a datetime object in UTC time.
- `timespan` - A timespan to search for, relative to the time of the request. Must match one of the API's timespan formats - https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/
- `num_records` - The number of records to return. Only used in article list mode and can be up to 250.
- `keyword` - Return articles containing the exact phrase `keyword` within the article text.
- `domain` - Return articles from the specified domain. Does not require an exact match so passing "cnn.com" will match articles from `cnn.com`, `subdomain.cnn.com` and `notactuallycnn.com`.
- `domain_exact` - Similar to `domain`, but requires an exact match.
- `country` - Return articles published in a country or list of countries, formatted as the FIPS 2 letter country code.
- `language` - Return articles published in the given language, formatted as the ISO 639 language code.
- `theme` - Return articles that cover one of GDELT's GKG Themes. A full list of themes can be found [here](http://data.gdeltproject.org/api/v2/guides/LOOKUP-GKGTHEMES.TXT)
- `near` - Return articles containing words close to each other in the text. Use `near()` to construct. eg. `near = near(5, "airline", "climate")`, or `multi_near()` if you want to use multiple restrictions eg. `multi_near([(5, "airline", "crisis"), (10, "airline", "climate", "change")], method="AND")` finds "airline" and "crisis" within 5 words, and "airline", "climate", and "change" within 10 words
- `repeat` - Return articles containing a single word repeated at least a number of times. Use `repeat()` to construct. eg. `repeat =repeat(3, "environment")`, or `multi_repeat()` if you want to use multiple restrictions eg. `repeat = multi_repeat([(2, "airline"), (3, "airport")], "AND")`
- `tone` - Return articles above or below a particular tone score (ie more positive or more negative than a certain threshold). To use, specify either a greater than or less than sign and a positive or negative number (either an integer or floating point number). To find fairly positive articles, use `tone=">5"` or to search for fairly negative articles, use `tone="<-5"`
- tone_absolute - The same as `tone` but ignores the positive/negative sign and lets you search for high emotion or low emotion articles, regardless of whether they were happy or sad in tone

## Attribution

The JSON schema data files in this package (`src/gdelt_client/data/schemas/`) are based on schemas from [gdeltPyR](https://github.com/linwoodc3/gdeltPyR), which is licensed under the GNU General Public License v3.0.

## Developing gdelt-client

PRs & issues are very welcome!

### Setup

It's recommended to use a virtual environment for development. Set one up with [uv](https://docs.astral.sh/uv/getting-started/installation/)

```
uv sync
```

Tests for this package use `pytest`. Run them with

```
uv run pytest tests --cov=src/gdelt_client --cov-report=xml --cov-report=term-missing
```

If your PR adds a new feature or helper, please also add some tests

### Publishing

There's a bit of automation set up to help publish a new version of the package to PyPI,

1. Make sure the version string has been updated since the last release. This package follows semantic versioning.
2. Create a new release in the Github UI, using the new version as the release name
3. Watch as the `publish.yml` Github action builds the package and pushes it to PyPI
