from quart import Quart
from API.oauth.routes import oauth_bp
# from API.migration.routes import migration_bp
# from API.reports.routes import reports_bp
# from API.errors.routes import errors_bp

class API:
    def __init__(self):
        self.app = Quart(__name__)
        self.routes = []

    def add_route(self, rule, endpoint=None, view_func=None, methods=None):
        """
        Add a route to the Quart app.

        Args:
            rule (str): The URL rule as a string.
            endpoint (str): The endpoint name (optional).
            view_func (callable): The function to call when serving this route.
            methods (list): The HTTP methods to allow (e.g., ['GET', 'POST']).
        """
        self.app.add_url_rule(rule, endpoint, view_func, methods=methods)
        self.routes.append({"rule": rule, "endpoint": endpoint, "methods": methods})

    async def run(self, *args, **kwargs):
        """
        Run the Quart app.

        Args:
            *args: Positional arguments for Quart's run method.
            **kwargs: Keyword arguments for Quart's run method.
                      If 'host' is not specified, defaults to '127.0.0.1'.
        """
        kwargs.setdefault('host', '127.0.0.1')  # Default to localhost if 'host' is not provided
        kwargs.setdefault('port', 5000)  # Default to port 5000 if not provided
        await self.app.run(*args, **kwargs)

    def get_registered_routes(self):
        """
        Get a list of registered routes for debugging or documentation.

        Returns:
            list: List of route dictionaries.
        """
        return self.routes


# Main application logic
async def main():
    api = API()

    # Register blueprints or routes dynamically
    api.app.register_blueprint(oauth_bp)  # Example: OAuth routes
    # api.app.register_blueprint(migration_bp)  # Migration routes
    # api.app.register_blueprint(reports_bp)    # Reports routes
    # api.app.register_blueprint(errors_bp)     # Errors routes

    print("Registered routes:")
    for route in api.get_registered_routes():
        print(route)

    # Run the app
    await api.run()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
