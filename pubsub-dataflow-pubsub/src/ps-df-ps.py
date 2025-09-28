import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io import filesystems
import json
from apache_beam.io.filesystems import FileSystems
from apache_beam.pvalue import TaggedOutput
from apache_beam import pvalue
import datetime
import logging



class ConfigOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--pipeline_config_path", required=True)
        parser.add_argument("--configs_options_path", required=True)

class ValidateMessageDoFn(beam.DoFn):
    def process(self, element):
        try:
            message = element.decode("utf-8")
            payload = json.loads(message)
            logging.info("Received JSON payload...")
            yield payload
        except json.JSONDecodeError as e:
            logging.error("Received invalid JSON payload...")
            yield beam.pvalue.TaggedOutput("invalidJson", message)

class WriteToGCSDoFn(beam.DoFn):

    def __init__(self, filename, prefix):
        self.filename = filename
        self.prefix = str(prefix).rstrip("/") + "/"
    def process(self, element):
        message = element

        if isinstance(message, (str, bytes, bytearray)):
            logging.info("Message type STR, BYTES or BYTEARRAY")
            message_str = element if isinstance(element, str) else element.decode("utf-8")
        else:
            logging.info("Message type JSON")
            message_str = json.dumps(element, ensure_ascii=False)


        timestamp = str(datetime.datetime.now().strftime("%Y%m%d%H%M%S")).replace(" ", "").replace(":","").replace(".","")
        out_name = self.filename + self.prefix + str(datetime.date.today()) + "/" + timestamp + ".json"

        try:
            with FileSystems.create(out_name) as f:
                f.write(message_str.encode("utf-8"))
            logging.info(f"Wrote {self.prefix} message to bucket")
        except Exception as e:
            logging.exception("Failed writing bad record: Error: %s", e)

class VerifyTrackingIDDoFn(beam.DoFn):
    def process(self, element):
        if "trackingId" in element and element["trackingId"] is not None:
            logging.info(f"Received Valid JSON payload with tracking ID: {element['trackingId']}")
            yield element
        else:
            logging.error(f"Received Valid JSON, but missing tracking ID")
            yield beam.pvalue.TaggedOutput("invalidTrackingID", element)

class LogMessageDoFn(beam.DoFn):
    def __init__(self, stage):
        self.stage = stage

    def process(self, element):
        logging.info("🔹 %s: %s", self.stage, element)
        yield element

def load_config(path):
    with filesystems.FileSystems.open(path) as f:
        return json.load(f)

def encode_bytes(o) -> bytes:
    if isinstance(o, (bytes, bytearray)):
        return bytes(o)
    return json.dumps(o, ensure_ascii=False).encode("utf-8")

def run():
    options = PipelineOptions()
    config_opts = options.view_as(ConfigOptions)

    # Load JSON configs
    pipeline_cfg = load_config(config_opts.pipeline_config_path)
    configs_options = load_config(config_opts.configs_options_path)

    runtime_options = PipelineOptions(
        project=pipeline_cfg["project"],
        region=pipeline_cfg["region"],
        runner=pipeline_cfg["runner"],
        temp_location=pipeline_cfg["temp_location"],
        staging_location=pipeline_cfg["staging_location"],
        streaming=pipeline_cfg.get("streaming", False),
    )
    runtime_options.view_as(StandardOptions).streaming = pipeline_cfg.get("streaming", False)


    subscription = f"projects/{configs_options['PROJECT_ID']}/subscriptions/{configs_options['INPUT_SUBSCRIPTION']}"

    if configs_options['OUTPUT_TOPIC'] is not None:    
        topic = f"projects/{configs_options['PROJECT_ID']}/topics/{configs_options['OUTPUT_TOPIC']}"
    
    

    with beam.Pipeline(options=runtime_options) as p:
        
        messages = (p | "Read from PubSub" >> beam.io.ReadFromPubSub(subscription=subscription))

        ValidateMessage = ( messages | "ValidateJSONReceived" >> beam.ParDo(ValidateMessageDoFn()).with_outputs("invalidJson", main="validJson"))

        if configs_options['WRITE_INVALID_MESSAGE_FLAG']:
            
            filename = f"gs://{configs_options['BUCKET_NAME']}/{configs_options['PREFIX']}/"
            ValidateMessage.invalidJson | "WriteInvalidJSONtoBucket" >> beam.ParDo(
                WriteToGCSDoFn(filename, configs_options['WRITE_INVALID_MESSAGE_FOLDER_NAME'])
            )

        CheckTrackingID = (
            ValidateMessage.validJson | "TrackingID" >> beam.ParDo(VerifyTrackingIDDoFn()).with_outputs("invalidTrackingID", main="validTrackingID"))

        if configs_options['WRITE_INVALID_TRACKINGID_MESSAGE_FLAG']:

            filename = f"gs://{configs_options['BUCKET_NAME']}/{configs_options['PREFIX']}/"
            CheckTrackingID.invalidTrackingID | "WriteMISSINGTrackingIDJSONtoBucket" >> beam.ParDo(
                WriteToGCSDoFn(filename, configs_options['WRITE_INVALID_TRACKINGID_MESSAGE_FOLDER_NAME'])
            )

        if configs_options['OUTPUT_TOPIC'] is None:
            _WriteToLogging = (CheckTrackingID.validTrackingID | "WriteJSONtoLOGGING" >> beam.ParDo(LogMessageDoFn("MESSAGE")))
        else:

            _WriteToTopic = ( CheckTrackingID.validTrackingID 
                | "EncodeToBytes" >> beam.Map(encode_bytes)
                | "WriteJSONtoPUBSUB" >> beam.io.WriteToPubSub(topic=topic)
            )
        
        if configs_options['WRITE_PROCESSED_MESSAGE_FLAG']:

            filename = f"gs://{configs_options['BUCKET_NAME']}/{configs_options['PREFIX']}/"
            _WriteToGCS = (
                CheckTrackingID.validTrackingID | "WriteJSONtoGCS" >> beam.ParDo( 
                    WriteToGCSDoFn(filename, configs_options['WRITE_PROCESSED_MESSAGE_FOLDER_NAME'])
                )
            )

# ---------- Entrypoint ----------
if __name__ == "__main__":
    run()
