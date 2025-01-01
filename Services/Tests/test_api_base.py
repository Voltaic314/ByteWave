import pytest
from aiohttp import ClientSession, ClientResponse
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from Services._api_base import APIBase


### Mocked Subclass for Testing ###
class MockAPIBase(APIBase):
    async def _refresh_token_from_service(self) -> dict:
        # Simulate token refresh
        return {
            "access_token": "new_access_token",
            "refresh_token": "new_refresh_token",
            "expires_in": 3600,
        }


@pytest.fixture
def mock_api():
    """Fixture for initializing the MockAPIBase with test request data."""
    request = {
        "access_token": "initial_access_token",
        "refresh_token": "initial_refresh_token",
        "expires_in": 3600,
        "service_name": "TestService",
    }
    return MockAPIBase(request)


@pytest.fixture
def mock_response():
    """Fixture for mocking a ClientResponse object."""
    response = MagicMock(spec=ClientResponse)
    response.text = AsyncMock(return_value="Error details")
    return response


### Tests ###
@pytest.mark.asyncio
async def test_token_expired(mock_api):
    # Simulate token not expired
    mock_api.token_expiration_time = datetime.now() + timedelta(seconds=3600)
    assert await mock_api.is_token_expired() is False

    # Simulate token expired
    mock_api.token_expiration_time = datetime.now() - timedelta(seconds=1)
    assert await mock_api.is_token_expired() is True


@pytest.mark.asyncio
async def test_refresh_token(mock_api):
    # Simulate token expiration and refresh
    mock_api.token_expiration_time = datetime.now() - timedelta(seconds=1)
    await mock_api.refresh_token()

    assert mock_api.access_token == "new_access_token"
    assert mock_api.refresh_token == "new_refresh_token"
    assert mock_api.token_expiration_time > datetime.now()


@pytest.mark.asyncio
async def test_handle_response_success(mock_api, mock_response):
    # Simulate a successful response
    mock_response.status = 200
    result = await mock_api.handle_response(mock_response)
    assert result == mock_response


@pytest.mark.asyncio
async def test_handle_response_unauthorized(mock_api, mock_response):
    # Simulate an unauthorized (401) response
    mock_response.status = 401
    await mock_api.refresh_token = AsyncMock()

    result = await mock_api.handle_response(mock_response)
    assert result is None
    await mock_api.refresh_token.assert_called_once()


@pytest.mark.asyncio
async def test_handle_response_error(mock_api, mock_response):
    # Simulate an error response
    mock_response.status = 404
    result = await mock_api.handle_response(mock_response)
    assert result is None


@pytest.mark.asyncio
async def test_make_request(mock_api, mock_response):
    # Mock the session request
    mock_api.session.request = AsyncMock(return_value=mock_response)

    # Simulate a successful first request
    mock_response.status = 200
    result = await mock_api.make_request("GET", "http://example.com")
    assert result == mock_response
    assert mock_api.total_api_calls_made == 2  # Initial OAuth + this request

    # Simulate an unauthorized response and successful retry
    mock_response.status = 401
    mock_api.refresh_token = AsyncMock()
    result = await mock_api.make_request("GET", "http://example.com")
    assert result == mock_response
    await mock_api.refresh_token.assert_called_once()
    assert mock_api.total_api_calls_made == 4  # OAuth + 2 requests + retry


@pytest.mark.asyncio
async def test_try_sdk_request(mock_api):
    # Mock the SDK method
    async def mock_method(*args, **kwargs):
        return {"success": True}

    result = await mock_api.try_sdk_request(mock_method)
    assert result == {"success": True}

    # Simulate a failure and retry
    async def failing_method(*args, **kwargs):
        raise Exception("401 Unauthorized")

    mock_api.refresh_token = AsyncMock()
    result = await mock_api.try_sdk_request(failing_method)
    assert result == {"error": "401 Unauthorized"}
    await mock_api.refresh_token.assert_called_once()
