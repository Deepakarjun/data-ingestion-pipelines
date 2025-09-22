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
        parser.add_argument("--io_options", required=True)


# ---------- Main Run Function ----------


def run():
    # Parse custom CLI options
    options = PipelineOptions()
    config_opts = options.view_as(ConfigOptions)

    # Load JSON configs
    pipeline_cfg = load_config(config_opts.pipeline_options)
    io_options = load_config(config_opts.io_options)

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


    if not io_options["input_bucket"].startswith("gs://"):
        raise ValueError("input_bucket must be a GCS path like gs://bucket/prefix")

    paths = {
        "invalid_filename": f"gs://{io_options['bucket_name']}/{io_options['processed_folder_name']}/invalid_filename",
        "invalid_json": f"gs://{io_options['bucket_name']}/{io_options['processed_folder_name']}/invalid_json",
        "invalid_data": f"gs://{io_options['bucket_name']}/{io_options['processed_folder_name']}/invalid_data",
        "valid_data": f"gs://{io_options['bucket_name']}/{io_options['processed_folder_name']}/valid_data"
    }

    with beam.Pipeline(options=runtime_options) as p:

        # 1) Continuously match new files
        matches = (p | "MatchContinuously" >> fileio.MatchContinuously(io_options["input_bucket"], interval=io_options["interval_sec"]))

        # 2) Read files
        files = matches | "ReadMatches" >> fileio.ReadMatches()

        # 3) File-level validation (name + extension + optional date)
        validatedFile = (files | "ValidateFiles" >> beam.ParDo(ValidateFileDoFn()).with_outputs("invalidFileName", main="validFileName"))

        # 4) Route invalid files to bad folders (move)
        validatedFile.invalidFileName | "MoveBadFilename" >> beam.ParDo(MoveFileDoFn(dest_prefix = paths["invalid_filename"]))

        # 5) Read records from valid files
        read_json = (validatedFile.validFileName | "ReadJsonRecords" >> beam.ParDo(ReadJsonRecordsDoFn()).with_outputs("invalidJson", main="validJson"))

        # 6) Write invalid records payloads
        read_json.invalidJson | "WriteInvalidData" >> beam.ParDo(WriteBadRecordDoFn(prefix=paths["invalid_json"]))

        # 7) Record-level schema validation
        validate_record_schema = (
            read_json.validJson
            | "ValidateSchema" >> beam.ParDo(
                ValidateRecordSchemaDoFn(schema_path=io_options["schema_path"])
            ).with_outputs("invalidSchema", main="validSchema")
        )

        validate_record_schema.invalidSchema | "WriteSchemaInvalid" >> beam.ParDo(WriteBadRecordDoFn(prefix=paths["invalid_data"]))

        # 8) Transform valid records
        transformed = validate_record_schema.validSchema | "Transform" >> beam.ParDo(TransformRecordDoFn())

        # 9) Publish to Pub/Sub
        _ = (
            transformed
            | "EncodeToBytes" >> beam.Map(encode_bytes)
            | "WriteToPubSub" >> beam.io.WriteToPubSub(topic=io_options["output_topic"])
        )

        # 10) Move processed files AFTER transformation branch emitted something.

        processed_paths = (
            read_json.validJson
            | "ExtractPaths" >> beam.ParDo(ExtractUniqueFilePaths())
            | "WindowForDedup" >> beam.WindowInto(window.FixedWindows(10))  # 1-minute window
            | "DeduplicatePaths" >> beam.Distinct()
        )

        _ = processed_paths | "MoveProcessed" >> beam.ParDo(MoveProcessedPathDoFn(paths["valid_data"]))

    # End with context manager (waits for streaming workers to start)

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
