import base64
import functions_framework
import requests
import logging
import os

logging.basicConfig(level=logging.INFO)

# Example: API URL you want to call
API_URL = os.getenv("API_URL")
CLIENT_IDENTIFIER = os.getenv("CLIENT_IDENTIFIER")

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
