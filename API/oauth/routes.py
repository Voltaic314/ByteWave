from flask import Blueprint, request, redirect, jsonify
from API.oauth.utils import save_tokens
import requests

oauth_bp = Blueprint("oauth", __name__)

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
def initiate_oauth(service):
    if service not in OAUTH_CONFIG:
        return jsonify({"error": f"Service '{service}' not supported"}), 400

    config = OAUTH_CONFIG[service]
    auth_url = (
        f"{config['auth_url']}?"
        f"client_id={config['client_id']}&"
        f"redirect_uri={config['redirect_uri']}&"
        f"response_type=code&"
        f"scope={config['scope']}"
    )
    return redirect(auth_url)

@oauth_bp.route("/callback/<service>", methods=["GET"])
def oauth_callback(service):
    if service not in OAUTH_CONFIG:
        return jsonify({"error": f"Service '{service}' not supported"}), 400

    code = request.args.get("code")
    if not code:
        return jsonify({"error": "Authorization code not provided"}), 400

    config = OAUTH_CONFIG[service]
    token_response = requests.post(
        config["token_url"],
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": config["redirect_uri"],
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
        },
    )

    if token_response.status_code != 200:
        return jsonify({"error": "Failed to exchange authorization code for tokens"}), 400

    tokens = token_response.json()
    save_tokens(service, tokens)
    return jsonify({"message": f"Tokens for '{service}' stored successfully!"})
