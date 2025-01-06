import json
import os

TOKENS_FILE = "tokens.json"

def save_tokens(service, tokens):
    """
    Save tokens securely.
    """
    if not os.path.exists(TOKENS_FILE):
        data = {}
    else:
        with open(TOKENS_FILE, "r") as file:
            data = json.load(file)

    data[service] = tokens
    with open(TOKENS_FILE, "w") as file:
        json.dump(data, file, indent=4)
