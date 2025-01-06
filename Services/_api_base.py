import asyncio
from aiohttp import ClientSession
from Helpers.db import DB
from Helpers.token_manager import TokenManager  

class APIBase:
    def __init__(self, request: dict, db: DB, roles: list = []):
        """
        Initialize the APIBase with token management and session setup.

        Args:
            request (dict): Initial request dictionary containing token info and service name.
            db (DB): Database instance for storing and managing tokens.
            roles (list): List of roles (e.g., ["source", "destination"]).
        """
        self.service_name = request.get("service_name", "Unknown Service")
        self.session = ClientSession()
        self.db = db
        self.roles = roles

        # Initialize TokenManager
        self.token_manager = TokenManager(
            db=db,
            service_name=self.service_name,
            refresh_token_url=request.get("refresh_token_url"),
            roles=self.roles,
            client_id=request.get("client_id"),
            client_secret=request.get("client_secret"),
            scope=request.get("scope", "default_scope"),
            redirect_uri=request.get("redirect_uri"),
        )

        # Set the initial token info
        self.access_token = None

    async def refresh_token(self):
        """Use the TokenManager to refresh and update the token."""
        self.access_token = await self.token_manager.get_token()

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
        if self.access_token is None:
            await self.refresh_token()

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
                    return result

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
            await asyncio.sleep(1)  # Optional backoff between retries
