from aiohttp import ClientResponse
from requests import Response

from gdelt_client.enums import HttpResponseCodes


class GdeltAPIError(Exception):
    """Base exception for GDELT API errors"""

    def __init__(self, response: Response | ClientResponse):
        self.response = response
        status_code = response.status_code if isinstance(response, Response) else response.status
        super().__init__(f"HTTP {status_code}: {response.reason}")


class BadRequestError(GdeltAPIError):
    """Raised when the response from the API is a 400 status"""


class NotFoundError(GdeltAPIError):
    """Raised when the response from the API is a 404 status"""


class RateLimitError(GdeltAPIError):
    """Raised when the response from the API is a 429 status"""


class ClientRequestError(GdeltAPIError):
    """Raised when the response from the API is a 4XX status that's not 400, 404 or 429"""


class ServerError(GdeltAPIError):
    """Raised when the response from the API is a 5XX status"""


def raise_response_error(response: Response | ClientResponse) -> None:
    status_code = response.status_code if isinstance(response, Response) else response.status
    if status_code == HttpResponseCodes.OK.value:
        return

    elif status_code == HttpResponseCodes.BAD_REQUEST.value:
        raise BadRequestError(response=response)

    elif status_code == HttpResponseCodes.NOT_FOUND.value:
        raise NotFoundError(response=response)

    elif status_code == HttpResponseCodes.RATE_LIMIT.value:
        raise RateLimitError(response=response)

    elif status_code >= 400 and status_code < 500:
        raise ClientRequestError(response=response)

    elif status_code >= 500 and status_code < 600:
        raise ServerError(response=response)

    else:
        raise GdeltAPIError(response=response)
