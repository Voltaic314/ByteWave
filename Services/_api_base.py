import asyncio
from aiohttp import ClientSession
from datetime import datetime, timedelta


class APIBase:
    def __init__(self, request: dict):
        # Initialize session and token attributes
        self.session = ClientSession()
        self.access_token = request.get("access_token")
        self.refresh_token = request.get("refresh_token")
        self.token_expiration_time = datetime.now() + timedelta(seconds=request.get("expires_in", 3600))
        self.service_name = request.get("service_name", "Unknown Service")
        self.total_api_calls_made = 1 # we're assuming initial oauth will just be 1 here.

    async def is_token_expired(self) -> bool:
        """Check if the token is expired."""
        return datetime.now() >= self.token_expiration_time

    async def refresh_token(self):
        """Refresh the API token."""
        if await self.is_token_expired():
            print(f"Token expired for {self.service_name}. Refreshing...")
            new_token_data = await self._refresh_token_from_service()
            self.access_token = new_token_data["access_token"]
            self.refresh_token = new_token_data["refresh_token"]
            self.token_expiration_time = datetime.now() + timedelta(seconds=new_token_data["expires_in"])

    async def _refresh_token_from_service(self) -> dict:
        """Subclasses must implement this method to handle their specific token refresh logic."""
        raise NotImplementedError("Subclasses must implement this method.")

    async def handle_response(self, response):
        """
        Handle the response from an API call.
        If unauthorized (401), refresh the token and retry.
        """
        if response.status == 401:
            print(f"Token expired for {self.service_name}. Attempting refresh...")
            await self.refresh_token()
            return None  # Indicate retry is needed

        # Handle non-critical error responses (e.g., 404 Not Found)
        if response.status >= 400:
            error_message = await response.text()
            print(f"Error from {self.service_name}: {response.status} - {error_message}")
            return None

        return response

    async def make_request(self, method: str, url: str, headers=None, **kwargs):
        """
        Central method for making network requests and processing responses.
        """
        headers = headers or {}
        headers["Authorization"] = f"Bearer {self.access_token}"

        async with self.session.request(method, url, headers=headers, **kwargs) as response:
            result = await self.handle_response(response)

            if result is None:
                # Retry the request after refreshing the token
                print("Retrying the request...")
                headers["Authorization"] = f"Bearer {self.access_token}"
                async with self.session.request(method, url, headers=headers, **kwargs) as retry_response:
                    result = await self.handle_response(retry_response)
                    self.total_api_calls_made += 1
            
            self.total_api_calls_made += 1
            return result

    async def try_sdk_request(self, method, *args, **kwargs):
        """
        Attempt to make an asynchronous request using an SDK.
        If the request fails, handle errors (e.g., token expiration) and retry.
        
        Args:
            method (callable): The SDK method to invoke.
            *args: Positional arguments for the SDK method.
            **kwargs: Keyword arguments for the SDK method.
        
        Returns:
            The response from the SDK method, or an error dictionary if retries fail.
        """
        attempts = 0
        max_attempts = 3

        while attempts < max_attempts:
            try:
                # Await the SDK method if it is async
                return await asyncio.to_thread(method, *args, **kwargs)
            except Exception as e:
                if "401" in str(e):
                    print("Token expired, attempting to refresh...")
                    await self.refresh_token()

                print(f"Exception while making SDK request: {str(e)}")
                
                # If max attempts are reached, return an error dictionary
                if attempts == max_attempts - 1:
                    print(f"Failed to make SDK request after {max_attempts} attempts.")
                    return {"error": str(e)}
            
            attempts += 1
            self.total_api_calls_made += 1
            await asyncio.sleep(1)  # Optional backoff between retries