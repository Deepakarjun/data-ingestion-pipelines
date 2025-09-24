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
from apache_beam import DoFn

class TransformRecordDoFn(beam.DoFn):
    """
    Placeholder for your business transformation.
    Input: (file_path, record_dict)
    Output: transformed_dict
    """
    def process(self, item):
        _, rec = item
        # EXAMPLE transformation: rename timestamp->event_ts, add processed_at
        transformed = dict(rec)
        yield transformed
        # if "timestamp" in transformed:
        #     transformed["event_ts"] = transformed.pop("timestamp")
        # transformed["processed_at"] = datetime.utcnow().isoformat() + "Z"
        # yield transformed