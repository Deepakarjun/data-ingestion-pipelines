# main.py
import argparse
import json
import logging
import re
from datetime import datetime
from typing import Iterable, Tuple

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.pvalue import TaggedOutput

from apache_beam import window

from costco.utilities.ExtractUniqueFiles import ExtractUniqueFilePaths
from costco.utilities.MoveFiles import MoveFileDoFn
from costco.utilities.MoveProcessedPath import MoveProcessedPathDoFn
from costco.utilities.ReadJsonRecords import ReadJsonRecordsDoFn
from costco.utilities.TransformRecord import TransformRecordDoFn
from costco.utilities.WriteBadRecords import WriteBadRecordDoFn
from costco.validations.ValidateFiles import ValidateFileDoFn
from costco.validations.ValidateRecords import ValidateRecordSchemaDoFn


def basename(path: str) -> str:
    # Works for gs:// and local paths
    return path.rstrip("/").split("/")[-1]

def join_gcs(*parts: str) -> str:
    left = "/".join(p.strip("/") for p in parts)
    if parts[0].startswith("gs://"):
        return "gs://" + left.split("gs://", 1)[1]
    return left

def encode_bytes(o) -> bytes:
    if isinstance(o, (bytes, bytearray)):
        return bytes(o)
    return json.dumps(o, ensure_ascii=False).encode("utf-8")

def load_config(path):
    with FileSystems.open(path) as f:
        return json.load(f)


# ---------- Custom Options ----------
class ConfigOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--pipeline_options", required=True)
        # parser.add_argument("--io_options", required=True)
        parser.add_argument("--configs_options", required=True)

def run():
    # Parse custom CLI options
    options = PipelineOptions()
    config_opts = options.view_as(ConfigOptions)

    # Load JSON configs
    pipeline_cfg = load_config(config_opts.pipeline_options)
    # io_options = load_config(config_opts.io_options)
    configs_options = load_config(config_opts.configs_options)

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

    if configs_options['BUCKET_NAME'] is None or configs_options['PREFIX'] is None or configs_options['INPUT_FOLDER'] is None:
        raise ValueError("Check your CONFIGURATION file for BUCKET_NAME, PREFIX and INPUT_FOLDER, it must have not NULL values")

    paths = {
        f"invalid_{configs_options['FILE_TYPE']}":  f"gs://{configs_options['BUCKET_NAME']}/{configs_options['PREFIX']}/{configs_options['INVALID_DATA_FOLDER']}/invalid_{configs_options['FILE_TYPE']}",
        "invalid_data":  f"gs://{configs_options['BUCKET_NAME']}/{configs_options['PREFIX']}/{configs_options['INVALID_DATA_FOLDER']}/invalid_data",
        "valid_data":    f"gs://{configs_options['BUCKET_NAME']}/{configs_options['PREFIX']}/{configs_options['PROCESSED_DATA_FOLDER']}",
        "tracking_id":   f"gs://{configs_options['BUCKET_NAME']}/{configs_options['PREFIX']}/{configs_options['TRACKING_ID_FOLDER']}"
    }

    file_path = f"gs://{configs_options['BUCKET_NAME']}/{configs_options['PREFIX']}/{configs_options['INPUT_FOLDER']}/*.{configs_options['FILE_TYPE']}"

    with beam.Pipeline(options=runtime_options) as p:

        matches = (p | f"Match {configs_options['FILE_TYPE']} Continuously" >> fileio.MatchContinuously(file_path, interval=configs_options["INTERVAL_SEC"]))

        files = matches | f"Read Matched {configs_options['FILE_TYPE']} Files" >> fileio.ReadMatches()

        validatFileNamePattern = ( files | f"Validate {configs_options['FILE_TYPE']} Files" >> beam.ParDo(ValidateFileDoFn(
                    configs_options["FILE_NAME_PATTERN"])
            ).with_outputs("invalidFileNamePattern", main="validFileNamePattern")
        )

        validatFileNamePattern.invalidFileNamePattern | "MoveBadFilename" >> beam.ParDo(MoveFileDoFn(dest_prefix = paths[f"invalid_{configs_options['FILE_TYPE']}"]))

        json_records = (validatFileNamePattern.validFileNamePattern | "ReadJsonRecords" >> beam.ParDo(ReadJsonRecordsDoFn()).with_outputs("invalidJson", main="validJson"))

        json_records.invalidJson | "WriteInvalidData" >> beam.ParDo(WriteBadRecordDoFn(prefix=paths['invalid_data']))

        validateJSONRecords = ( json_records.validJson | "ValidateSchema" >> beam.ParDo(
                ValidateRecordSchemaDoFn(schema_path=configs_options["SCHEMA_PATH"])
            ).with_outputs("TrackingIdIssue", main="validSchema")
        )

        validateJSONRecords.TrackingIdIssue | "WriteSchemaInvalid" >> beam.ParDo(WriteBadRecordDoFn(prefix=paths["tracking_id"]))

        # transformed = validateJSONRecords.validSchema | "Transform" >> beam.ParDo(TransformRecordDoFn())

        _ = (
            # transformed
            validateJSONRecords.validSchema
            | "EncodeToBytes" >> beam.Map(encode_bytes)
            | "WriteToPubSub" >> beam.io.WriteToPubSub(topic=f"projects/{configs_options['PROJECT_ID']}/topics/{configs_options['OUTPUT_TOPIC']}")
        )

        processed_paths = (
            json_records.validJson
            | "ExtractPaths" >> beam.ParDo(ExtractUniqueFilePaths())
            | "WindowForDedup" >> beam.WindowInto(window.FixedWindows(10))  # 1-minute window
            | "DeduplicatePaths" >> beam.Distinct()
        )

        _ = processed_paths | "MoveProcessed" >> beam.ParDo(MoveProcessedPathDoFn(paths["valid_data"]))

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
