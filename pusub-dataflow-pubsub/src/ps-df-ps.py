import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io import filesystems
import json

# ---------- DoFns ----------
class DuplicateMessage(beam.DoFn):
    def __init__(self, repeat_count=2):
        self.repeat_count = repeat_count

    def process(self, element):
        if element:
            message = element.decode("utf-8")
            transformed = " ".join([message] * self.repeat_count)
            yield transformed.encode("utf-8")
        else:
            yield b""


class UppercaseMessage(beam.DoFn):
    def process(self, element):
        if element:
            message = element.decode("utf-8").upper()
            yield message.encode("utf-8")
        else:
            yield b""


# ---------- Config Loader ----------
def load_config(path):
    with filesystems.FileSystems.open(path) as f:
        return json.load(f)


# ---------- Custom Options ----------
class ConfigOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--pipeline_config_path", required=True)
        parser.add_argument("--io_config_path", required=True)
        parser.add_argument("--transform_config_path", required=True)


# ---------- Main Run Function ----------
def run():
    # Parse custom CLI options
    options = PipelineOptions()
    config_opts = options.view_as(ConfigOptions)

    # Load JSON configs
    pipeline_cfg = load_config(config_opts.pipeline_config_path)
    io_cfg = load_config(config_opts.io_config_path)
    transform_cfg = load_config(config_opts.transform_config_path)

    # Build Dataflow runtime options
    runtime_options = PipelineOptions(
        project=pipeline_cfg["project"],
        region=pipeline_cfg["region"],
        runner=pipeline_cfg["runner"],
        temp_location=pipeline_cfg["temp_location"],
        staging_location=pipeline_cfg["staging_location"],
        streaming=pipeline_cfg.get("streaming", False),
    )
    runtime_options.view_as(StandardOptions).streaming = pipeline_cfg.get("streaming", False)

    # Define pipeline
    with beam.Pipeline(options=runtime_options) as p:
        messages = (
            p
            | "Read from PubSub" >> beam.io.ReadFromPubSub(subscription=io_cfg["input_subscription"])
        )

        # Apply transforms conditionally
        if transform_cfg["duplicate"]["enabled"]:
            messages = messages | "Duplicate" >> beam.ParDo(
                DuplicateMessage(repeat_count=transform_cfg["duplicate"].get("repeat_count", 2))
            )

        if transform_cfg["uppercase"]["enabled"]:
            messages = messages | "Uppercase" >> beam.ParDo(UppercaseMessage())

        messages | "Write to PubSub" >> beam.io.WriteToPubSub(topic=io_cfg["output_topic"])


# ---------- Entrypoint ----------
if __name__ == "__main__":
    run()
