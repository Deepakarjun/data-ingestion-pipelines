import apache_beam as beam
from apache_beam import DoFn
from apache_beam.io import fileio
from typing import Tuple
import re

class ValidateRecordSchemaDoFn(beam.DoFn):
    """
    Validates required keys exist in the record dict.
    Emits:
      - main: (file_path, valid_record_dict)
      - 'invalid': (file_path, record_dict, reason)
    """
    def __init__(self, required_keys: list[str]):
        self.required_keys = required_keys

    def process(self, item: Tuple[str, dict]):
        path, rec = item
        missing = [k for k in self.required_keys if k not in rec]
        if missing:
            yield TaggedOutput("invalid", (path, rec, f"missing keys: {missing}"))
        else:
            yield (path, rec)