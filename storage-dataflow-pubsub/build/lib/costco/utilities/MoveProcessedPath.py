# import argparse
# import json
# import re
# from typing import Iterable, Tuple
# import os
# from apache_beam.io import fileio
# from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
# from apache_beam.pvalue import TaggedOutput

import logging
import apache_beam as beam
from apache_beam import DoFn
from datetime import datetime
from apache_beam.io.filesystems import FileSystems
from os.path import basename

class MoveProcessedPathDoFn(beam.DoFn):
    """
    Moves a file path (string) to processed/ folder once downstream stage emitted an element.
    """
    def __init__(self, dest_prefix: str):
        self.dest_prefix = dest_prefix.rstrip("/") + "/"

    def process(self, path: str):
        try:
            dst = self.dest_prefix+ str(datetime.now().strftime("%Y-%m-%d")) + "/" + basename(path)
            FileSystems.copy([path], [dst])
            FileSystems.delete([path])
            logging.info("Processed move %s -> %s", path, dst)
        except Exception as e:
            logging.exception("Failed processed move %s: %s", path, e)