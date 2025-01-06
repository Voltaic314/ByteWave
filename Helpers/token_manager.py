import asyncio
from datetime import datetime, timedelta
from Helpers.db import DB


class TokenManager:
    def __init__(self, db: DB, service_name: str, refresh_token_url: str, roles: list = [], **kwargs):
        """
        Initialize a generic TokenManager.

        Args:
            db (DB): The database instance.
            service_name (str): The name of the service being managed.
            refresh_token_url (str): The URL to refresh the token.
            **kwargs: Additional parameters for OAuth, such as client_id, client_secret, etc.
        """
        self.db = db
        self.service_name = service_name
        self.refresh_token_url = refresh_token_url
        self.roles = roles

        # Handle additional keyword arguments for flexibility
        self.client_id = kwargs.get("client_id")
        self.client_secret = kwargs.get("client_secret")
        self.scope = kwargs.get("scope", "default_scope")
        self.redirect_uri = kwargs.get("redirect_uri")
        self.token_cache = None

        # Locks for thread safety
        self.cache_lock = asyncio.Lock()
        self.refresh_lock = asyncio.Lock()

    async def get_token(self):
        """
        Retrieve the current token, refreshing it if necessary.

        Returns:
            str: The current access token.
        """
        async with self.cache_lock:
            if self.token_cache is None:
                await self._load_token_from_db()

            if await self._is_token_expired():
                await self._refresh_token()

            return self.token_cache["access_token"]

    async def _load_token_from_db(self):
        """
        Load token information from the database into the cache.
        """
        auth_info = await self.db.fetch_auth_info(self.service_name)
        if not auth_info:
            raise ValueError(f"No auth info found for service: {self.service_name}")

        self.token_cache = {
            "access_token": auth_info["access_token"],
            "refresh_token": auth_info["refresh_token"],
            "expiry": datetime.fromtimestamp(auth_info["token_expiry"]),
        }

    async def _is_token_expired(self) -> bool:
        """
        Check if the token is expired.

        Returns:
            bool: True if the token is expired, False otherwise.
        """
        return datetime.now() >= self.token_cache["expiry"]

    async def _refresh_token(self):
        """
        Refresh the access token, ensuring only one process refreshes at a time.
        """
        async with self.refresh_lock:
            if not await self._is_token_expired():  # Double-check token expiration
                return

            refreshed_token_data = await self._refresh_token_from_service()
            self.token_cache.update({
                "access_token": refreshed_token_data["access_token"],
                "refresh_token": refreshed_token_data.get("refresh_token", self.token_cache["refresh_token"]),
                "expiry": datetime.now() + timedelta(seconds=refreshed_token_data["expires_in"]),
            })

            await self.db.upsert_auth_info(
                service=self.service_name,
                access_token=self.token_cache["access_token"],
                refresh_token=self.token_cache["refresh_token"],
                token_expiry=int(self.token_cache["expiry"].timestamp()),
                roles=self.roles,
            )
