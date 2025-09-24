import apache_beam as beam
import json
from google.cloud import storage
from typing import Tuple
from apache_beam.options.value_provider import ValueProvider

class ValidateRecordSchemaDoFn(beam.DoFn):
    def __init__(self, schema_path: ValueProvider):
        self.schema_path = schema_path
        self.required_keys = []
        self.schema_fields = []

    def setup(self):
        schema_path = self.schema_path

        # If schema_path is a ValueProvider (runtime parameter)
        if isinstance(schema_path, ValueProvider):
            schema_path = schema_path.get()

        if not schema_path.startswith("gs://"):
            raise ValueError("Schema path must start with gs:// or path for schema_file in GCS")

        bucket_name, blob_name = schema_path[5:].split("/", 1)

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        schema_json = blob.download_as_text()
        schema = json.loads(schema_json)

        self.schema_fields = schema
        self.required_keys = [f["name"] for f in schema if f.get("mode", "NULLABLE") == "REQUIRED"]

    def process(self, item: Tuple[str, dict]):
        path, rec = item

        # Check required keys
        missing = [k for k in self.required_keys if k not in rec]
        if missing:
            yield beam.pvalue.TaggedOutput("invalid", (path, rec, f"Missing required keys: {missing}"))
            return

        # Type validation
        type_errors = []
        for field in self.schema_fields:
            name = field["name"]
            expected_type = field["type"]

            if name in rec:
                value = rec[name]
                if not self._validate_type(value, expected_type):
                    type_errors.append(f"{name} expected {expected_type}, got {type(value).__name__}")

        if type_errors:
            yield beam.pvalue.TaggedOutput("invalid", (path, rec, f"Type errors: {type_errors}"))
        else:
            yield (path, rec)

    def _validate_type(self, value, expected_type: str) -> bool:
        if expected_type == "STRING":
            return isinstance(value, str)
        if expected_type == "INTEGER":
            return isinstance(value, int)
        if expected_type == "FLOAT":
            return isinstance(value, (int, float))
        if expected_type == "BOOLEAN":
            return isinstance(value, bool)
        if expected_type == "TIMESTAMP":
            return isinstance(value, str)  # could enhance with datetime parsing
        return True
