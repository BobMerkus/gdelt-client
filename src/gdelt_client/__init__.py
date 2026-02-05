from gdelt_client.api_client import GdeltClient
from gdelt_client.enums import ArticleMode, GdeltTable, OutputFormat, TimeSeriesMode
from gdelt_client.errors import (
    BadRequestError,
    ClientRequestError,
    GdeltAPIError,
    NotFoundError,
    RateLimitError,
    ServerError,
)
from gdelt_client.filters import (
    VALID_TIMESPAN_UNITS,
    Filters,
    multi_near,
    multi_repeat,
    near,
    repeat,
)

# Alias for backward compatibility with `gdeltdoc` https://github.com/alex9smith/gdelt-doc-api
GdeltDoc = GdeltClient

__all__ = [
    "VALID_TIMESPAN_UNITS",
    "ArticleMode",
    "BadRequestError",
    "ClientRequestError",
    "Filters",
    "GdeltAPIError",
    "GdeltClient",
    "GdeltDoc",
    "GdeltTable",
    "NotFoundError",
    "OutputFormat",
    "RateLimitError",
    "ServerError",
    "TimeSeriesMode",
    "multi_near",
    "multi_repeat",
    "near",
    "repeat",
]
