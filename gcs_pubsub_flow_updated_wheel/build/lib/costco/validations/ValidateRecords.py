import apache_beam as beam
import json
from google.cloud import storage
from typing import Tuple
from apache_beam.options.value_provider import ValueProvider
import os
import logging
from pathlib import Path
import importlib.resources as pkg_resources
import schemas

class ValidateRecordSchemaDoFn(beam.DoFn):

    # def __init__(self, schema_path: ValueProvider):
    #     self.schema_path = schema_path
    #     self.required_keys = []
    #     self.schema_fields = []

    # def setup(self):
    #     try:
    #         with pkg_resources.files(schemas).joinpath("default_schema.json").open("r") as f:
    #             self.schema_fields = json.load(f)

    #         self.required_keys = [
    #             f["name"] for f in self.schema_fields if f.get("mode", "NULLABLE") == "REQUIRED"
    #         ]

    #         logging.info(
    #             f"[ValidateRecordSchemaDoFn] Loaded bundled schema with "
    #             f"{len(self.schema_fields)} fields. Required: {self.required_keys}"
    #         )

    #     except Exception as e:
    #         logging.error(f"[ValidateRecordSchemaDoFn] Failed to load bundled schema: {e}")
    #         raise

    def process(self, item: Tuple[str, dict]):
        path, rec = item

        if "trackingId" not in rec or rec["trackingId"] is None:
            yield pvalue.TaggedOutput("TrackingIdIssue", (path,rec))
        else:
            yield (path, rec)

    #     # Check required keys
    #     missing = [k for k in self.required_keys if k not in rec]
    #     if missing:
    #         yield beam.pvalue.TaggedOutput("invalidSchema", (path, rec, f"Missing required keys: {missing}"))
    #         return

    #     # Type validation
    #     type_errors = []
    #     for field in self.schema_fields:
    #         name = field["name"]
    #         expected_type = field["type"]

    #         if name in rec:
    #             value = rec[name]
    #             if not self._validate_type(value, expected_type):
    #                 type_errors.append(f"{name} expected {expected_type}, got {type(value).__name__}")

    #     if type_errors:
    #         yield beam.pvalue.TaggedOutput("invalidSchema", (path, rec, f"Type errors: {type_errors}"))
    #     else:
    #         yield (path, rec)

    # def _validate_type(self, value, expected_type: str) -> bool:
    #     if expected_type == "STRING":
    #         return isinstance(value, str)
    #     if expected_type == "INTEGER":
    #         return isinstance(value, int)
    #     if expected_type == "FLOAT":
    #         return isinstance(value, (int, float))
    #     if expected_type == "BOOLEAN":
    #         return isinstance(value, bool)
    #     if expected_type == "TIMESTAMP":
    #         return isinstance(value, str)  # could enhance with datetime parsing
    #     return True
