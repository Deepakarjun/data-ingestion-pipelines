# import argparse
# import json
# import re
# from typing import Iterable, Tuple
# import os
# from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
# from apache_beam.pvalue import TaggedOutput


from apache_beam.io import fileio
from apache_beam.io.filesystems import FileSystems
from os.path import basename
import apache_beam as beam
from apache_beam import DoFn
import datetime
import logging


class MoveFileDoFn(beam.DoFn):
    """
    Copies file to <dest_prefix>/<filename> and deletes the original.
    """
    def __init__(self, dest_prefix: str):
        self.dest_prefix = dest_prefix.rstrip("/") + "/"

    def process(self, readable_file: fileio.ReadableFile):

        src = readable_file.metadata.path
        dst = self.dest_prefix + str(datetime.datetime.now().strftime("%Y-%m-%d")) + "/" + basename(src)

        try:
            with FileSystems.open(src):
                pass
            FileSystems.copy([src], [dst])
            FileSystems.delete([src])
            logging.info("Moved %s -> %s", src, dst)
        except Exception as e:
            logging.exception("Failed moving %s to %s: %s", src, dst, e)