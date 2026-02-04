from .api_client import GdeltClient
from .errors import (
    BadRequestError,
    ClientRequestError,
    NotFoundError,
    RateLimitError,
    ServerError,
)
from .filters import (
    VALID_TIMESPAN_UNITS,
    Filters,
    multi_near,
    multi_repeat,
    near,
    repeat,
)

# Alias for backward compatibility
GdeltDoc = GdeltClient

__all__ = [
    "VALID_TIMESPAN_UNITS",
    "BadRequestError",
    "ClientRequestError",
    "Filters",
    "GdeltClient",
    "GdeltDoc",
    "NotFoundError",
    "RateLimitError",
    "ServerError",
    "multi_near",
    "multi_repeat",
    "near",
    "repeat",
]
