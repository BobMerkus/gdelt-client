from unittest import mock

import pytest
from aiohttp import ClientResponse

from gdelt_client.enums import HttpResponseCodes
from gdelt_client.errors import (
    BadRequestError,
    ClientRequestError,
    GdeltAPIError,
    NotFoundError,
    RateLimitError,
    ServerError,
    raise_response_error,
)


def build_response(status_code: int) -> ClientResponse:
    """Build a mock aiohttp ClientResponse for testing"""
    response = mock.AsyncMock(spec=ClientResponse)
    response.status = status_code
    response.reason = "Test Reason"
    response.headers = {"content-type": "application/json"}
    return response


class TestRaiseResponseError:
    def test_doesnt_raise_when_status_200(self):
        raise_response_error(build_response(HttpResponseCodes.OK.value))

    def test_raises_bad_request_error_when_status_400(self):
        with pytest.raises(BadRequestError):
            raise_response_error(build_response(HttpResponseCodes.BAD_REQUEST.value))

    def test_raises_not_found_error_when_status_404(self):
        with pytest.raises(NotFoundError):
            raise_response_error(build_response(HttpResponseCodes.NOT_FOUND.value))

    def test_raises_rate_limit_error_when_status_429(self):
        with pytest.raises(RateLimitError):
            raise_response_error(build_response(HttpResponseCodes.RATE_LIMIT.value))

    def test_raises_server_error_when_status_5XX(self):
        with pytest.raises(ServerError):
            raise_response_error(build_response(503))

    def test_raises_client_error_when_status_4XX(self):
        with pytest.raises(ClientRequestError):
            raise_response_error(build_response(403))

    def test_raises_http_error_when_status_unhandled(self):
        with pytest.raises(GdeltAPIError):
            raise_response_error(build_response(600))
