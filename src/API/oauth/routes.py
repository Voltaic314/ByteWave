from quart import Blueprint, request, redirect
from src import API_Response
from API.oauth.utils import save_tokens
import aiohttp

oauth_bp = Blueprint("oauth", __name__)

## TODO: Instead of hard coding this like an idiot, we need to store oauth credentials elsewhere
## probably would be a good idea to put them in a json file or in the DB, maybe a separate DB file for extra security
## This way idiots don't accidentally share their credentials with each other when sharing their DB file for the migration process. 
OAUTH_CONFIG = {
    "google_drive": {
        "auth_url": "https://accounts.google.com/o/oauth2/v2/auth",
        "token_url": "https://oauth2.googleapis.com/token",
        "client_id": "your_client_id",
        "client_secret": "your_client_secret",
        "redirect_uri": "http://localhost:5000/oauth/callback/google_drive",
        "scope": "https://www.googleapis.com/auth/drive.file",
    },
}


@oauth_bp.route("/initiate/<service>", methods=["POST"])
async def initiate_oauth(service):
    if service not in OAUTH_CONFIG:
        response = API_Response(success=False, code=400)
        response.add_error(
            error_type="ServiceError",
            message=f"Service '{service}' is not supported.",
            details="Ensure the service is properly configured in OAUTH_CONFIG."
        )
        return response

    config = OAUTH_CONFIG[service]
    auth_url = (
        f"{config['auth_url']}?"
        f"client_id={config['client_id']}&"
        f"redirect_uri={config['redirect_uri']}&"
        f"response_type=code&"
        f"scope={config['scope']}"
    )
    return API_Response(success=True, response=redirect(auth_url))


@oauth_bp.route("/callback/<service>", methods=["GET"])
async def oauth_callback(service):
    if service not in OAUTH_CONFIG:
        response = API_Response(success=False, code=400)
        response.add_error(
            error_type="ServiceError",
            message=f"Service '{service}' is not supported.",
            details="Ensure the service is properly configured in OAUTH_CONFIG."
        )
        return response

    code = request.args.get("code")
    if not code:
        response = API_Response(success=False, code=400)
        response.add_error(
            error_type="MissingCodeError",
            message="Authorization code not provided.",
            details="The authorization server did not return an authorization code."
        )
        return response

    config = OAUTH_CONFIG[service]
    async with aiohttp.ClientSession() as session:
        async with session.post(
            config["token_url"],
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": config["redirect_uri"],
                "client_id": config["client_id"],
                "client_secret": config["client_secret"],
            }
        ) as token_response:
            if token_response.status != 200:
                error_details = await token_response.text()
                response = API_Response(success=False, code=400)
                response.add_error(
                    error_type="TokenExchangeError",
                    message="Failed to exchange authorization code for tokens.",
                    details=error_details
                )
                return response

            tokens = await token_response.json()
            save_tokens(service, tokens)
            return API_Response(
                success=True,
                response=f"Tokens for '{service}' stored successfully!"
            )