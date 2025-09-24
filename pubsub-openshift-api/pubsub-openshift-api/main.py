import base64
import functions_framework
import requests
import logging
import os
import json

# logging.basicConfig(level=logging.INFO)

log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, log_level, logging.INFO))


print("test user 1")

# Example: API URL you want to call
API_URL = os.getenv("API_URL")
CLIENT_IDENTIFIER = os.getenv("CLIENT_IDENTIFIER")

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "default_schema.json")

with open(SCHEMA_PATH, "r") as f:
    schema = json.load(f)

required_keys = [item["name"] for item in schema]

def validate_input(data):
    print(f"Required keys: {required_keys}")

    missing_keys = [key for key in required_keys if key not in data]
    if missing_keys:
        raise ValueError(f"Missing required keys: {missing_keys}")
    return True

@functions_framework.cloud_event
def pubsub_to_api(cloud_event):
    """
    Triggered from Pub/Sub message.
    Reads the message, then calls an external API (GET).
    """

    try:
        # Extract Pub/Sub message
        message = cloud_event.data.get("message", {})
        data = message.get("data")

        decoded_message = None
        if data:
            decoded_message = base64.b64decode(data).decode("utf-8")
            logging.info(f"Received Pub/Sub message: {decoded_message}")
        else:
            logging.warning("No data found in Pub/Sub message")

        headers = {
        "client-identifier": CLIENT_IDENTIFIER
        }

        try:
            payload = json.loads(decoded_message)
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON: {e}")
            logging.error(f"Received message: {decoded_message}")
            return "Invalid JSON"

        boolen = validate_input(payload)

        if boolen: 
            response = requests.get(API_URL, headers=headers, timeout=30, verify=True)
            response.raise_for_status()

            logging.info(f"API call success [{response.status_code}]: {response.text[:200]}")

            print(f"API Response: {response.text}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error calling API: {e}", exc_info=True)
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        raise
