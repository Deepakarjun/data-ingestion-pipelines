import base64
import functions_framework
import requests
import logging
import os
import json
import datetime
from json import JSONDecodeError
from google.cloud import storage


CONFIG_PATH = os.getenv("CONFIG_PATH")
API_URL = os.getenv("API_URL")
CLIENT_IDENTIFIER = os.getenv("CLIENT_IDENTIFIER")

PATH = os.path.join(os.path.dirname(__file__), CONFIG_PATH)
print(f"PATH: {PATH}")

storage_client = storage.Client()

try: 
    CONFIGS = json.load(open(PATH))
except JSONDecodeError as e:
    logging.error(f"Invalid JSON: {e}")
    raise

bucket = storage_client.bucket(CONFIGS["BUCKET_NAME"])

# log_level = os.getenv("LOG_LEVEL", "INFO").upper()
# logging.basicConfig(level=getattr(logging, log_level, logging.INFO))

# def POST
# def PUT
# def DELETE
# def GET

@functions_framework.cloud_event
def pubsub_to_api(cloud_event):

    message = cloud_event.data.get("message", {})
    data = message.get("data")
    decoded_message = None

    try:
        decoded_message = base64.b64decode(data).decode("utf-8")
    except (TypeError, ValueError, AttributeError) as e:
        if CONFIGS["INVALID_DECODE_DATA_FOLDER_FLAG"]:
            blob = bucket.blob(CONFIGS["PREFIX"]+"/"+CONFIGS["INVALID_DECODE_DATA_FOLDER"]+"/"+str(datetime.date.today())+"/"+str(datetime.datetime.now()).replace(" ","_")+".json")
            blob.upload_from_string(data)
        logging.error(f"Error decoding message: {e}", exc_info=True)
        raise

    try:
        payload = json.loads(decoded_message)
    except json.JSONDecodeError as e:
        if CONFIGS["INVALID_DATA_FOLDER_FLAG"]:
            blob = bucket.blob(CONFIGS["PREFIX"]+"/"+CONFIGS["INVALID_DATA_FOLDER"]+"/"+str(datetime.date.today())+"/"+str(datetime.datetime.now()).replace(" ","_")+".json")
            blob.upload_from_string(decoded_message)
        logging.error(f"Invalid JSON: {e}")
        raise

    if "trackingId" not in payload or payload["trackingId"] is None:
        if CONFIGS["TRACKING_ID_FOLDER_FLAG"]:
            blob = bucket.blob(CONFIGS["PREFIX"]+"/"+CONFIGS["TRACKING_ID_FOLDER"]+"/"+str(datetime.date.today())+"/"+str(datetime.datetime.now()).replace(" ","_")+".json")
            blob.upload_from_string(decoded_message)
        logging.error(f"KeyError... ")
        raise

    try:
        headers = {"client-identifier": CLIENT_IDENTIFIER}
        response = requests.get(API_URL, headers=headers, timeout=30, verify=True)
        response.raise_for_status()
    except Exception as e:
        if CONFIGS["API_RISPONSE_FAIDED_FOLDER_FLAG"]:
            blob = bucket.blob(CONFIGS["PREFIX"]+"/"+CONFIGS["API_RISPONSE_FAIDED_FOLDER"]+"/"+str(datetime.date.today())+"/"+str(datetime.datetime.now()).replace(" ","_")+".json")
            blob.upload_from_string(decoded_message)        
        logging.error(f"Unexpected error: {e}", exc_info=True)
        raise
    else:
        if CONFIGS["PROCESSED_DATA_FOLDER_FLAG"]:
            blob = bucket.blob(CONFIGS["PREFIX"]+"/"+CONFIGS["PROCESSED_DATA_FOLDER"]+"/"+str(datetime.date.today())+"/"+str(datetime.datetime.now()).replace(" ","_")+".json")
            blob.upload_from_string(decoded_message)
        logging.info(f"API call success [{response.status_code}]: {response.text[:200]}")
        # print(f"API Response: {response.text}")

    finally:
        print("processed message")