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
class ExtractUniqueFilePaths(beam.DoFn):
    """
    From (file_path, record_dict) emits just file_path to deduplicate later.
    """
    def process(self, item):
        path, _ = item
        yield path