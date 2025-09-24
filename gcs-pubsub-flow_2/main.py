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
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
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


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--schema_path",
        required=True,
        help="GCS folder to keep schema, e.g. gs://my-bucket/schema (without trailing slash ok)",
    )

    parser.add_argument(
        "--input_bucket",
        required=True,
        help="GCS folder to watch, e.g. gs://my-bucket/data (without trailing slash ok)",
    )
    parser.add_argument(
        "--output_topic",
        required=True,
        help="Pub/Sub topic, e.g. projects/PROJECT_ID/topics/TOPIC",
    )
    parser.add_argument(
        "--interval_sec",
        type=int,
        default=60,
        help="Polling interval for MatchContinuously in seconds",
    )
    parser.add_argument(
        "--expected_date_yyyymmdd",
        default=None,
        help="If set, require filenames to match this date (YYYYMMDD). "
             "Omit to accept any date in the pattern.",
    )
    parser.add_argument(
        "--required_keys",
        default="id,name,timestamp",
        help="Comma-separated required keys in each JSON record",
    )
    args, beam_args = parser.parse_known_args(argv)

    # Pipeline options
    options = PipelineOptions(beam_args, streaming=True, save_main_session=True)
    options.view_as(SetupOptions).save_main_session = True

    # Derive paths
    input_glob = args.input_bucket.rstrip("/") + "/*.json"  # glob for streaming
    # Bucket extraction
    if not args.input_bucket.startswith("gs://"):
        raise ValueError("input_bucket must be a GCS path like gs://bucket/prefix")
    bucket_name = args.input_bucket.split("/", 3)[2]

    bad_root = "bad_records"
    paths = {
        "bad_filename": f"gs://{bucket_name}/{bad_root}/mis-match_fileName",
        "bad_extension": f"gs://{bucket_name}/{bad_root}/mis-match_otherExtension",
        "invalid_data": f"gs://{bucket_name}/{bad_root}/invalid_data",
        "processed": f"gs://{bucket_name}/processed",
    }

    with beam.Pipeline(options=options) as p:
        # 1) Continuously match new files
        matches = (
            p
            | "MatchContinuously" >> fileio.MatchContinuously(
                input_glob, interval=args.interval_sec
            )
        )

        # 2) Read files
        files = matches | "ReadMatches" >> fileio.ReadMatches()

        # 3) File-level validation (name + extension + optional date)
        validated = (
            files
            | "ValidateFiles" >> beam.ParDo(
                ValidateFileDoFn(expected_date_yyyymmdd=args.expected_date_yyyymmdd)
            ).with_outputs("bad_filename", "bad_extension", main="valid")
        )

        # 4) Route invalid files to bad folders (move)
        validated.bad_filename | "MoveBadFilename" >> beam.ParDo(MoveFileDoFn(paths["bad_filename"]))
        validated.bad_extension | "MoveBadExt" >> beam.ParDo(MoveFileDoFn(paths["bad_extension"]))

        # 5) Read records from valid files
        read_records = (validated.valid | "ReadJsonRecords" >> beam.ParDo(ReadJsonRecordsDoFn()).with_outputs("invalid", main="records"))

        # 6) Write invalid records payloads
        read_records.invalid | "WriteInvalidData" >> beam.ParDo(WriteBadRecordDoFn(bucket=bucket_name, prefix=f"{bad_root}/invalid_data"))

        # 7) Record-level schema validation
        # checked = (read_records.records | "ValidateSchema" >> beam.ParDo(ValidateRecordSchemaDoFn(required_keys)).with_outputs("invalid", main="valid_records"))
        checked = (
            read_records.records
            | "ValidateSchema" >> beam.ParDo(
                ValidateRecordSchemaDoFn(schema_path=args.schema_path)
            ).with_outputs("invalid", main="valid_records")
        )

        checked.invalid | "WriteSchemaInvalid" >> beam.ParDo(WriteBadRecordDoFn(bucket=bucket_name, prefix=f"{bad_root}/invalid_data"))

        # 8) Transform valid records
        transformed = checked.valid_records | "Transform" >> beam.ParDo(TransformRecordDoFn())

        # 9) Publish to Pub/Sub
        _ = (
            transformed
            | "EncodeToBytes" >> beam.Map(encode_bytes)
            | "WriteToPubSub" >> beam.io.WriteToPubSub(topic=args.output_topic)
        )

        # 10) Move processed files AFTER transformation branch emitted something.

        processed_paths = (
            checked.valid_records
            | "ExtractPaths" >> beam.ParDo(ExtractUniqueFilePaths())
            | "WindowForDedup" >> beam.WindowInto(window.FixedWindows(60))  # 1-minute window
            | "DeduplicatePaths" >> beam.Distinct()
        )

        _ = processed_paths | "MoveProcessed" >> beam.ParDo(MoveProcessedPathDoFn(paths["processed"]))

    # End with context manager (waits for streaming workers to start)

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
