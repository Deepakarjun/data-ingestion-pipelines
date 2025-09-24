import apache_beam as beam
from apache_beam import DoFn
from apache_beam.io import fileio
import re
from os.path import basename
from apache_beam import pvalue

class ValidateFileDoFn(beam.DoFn):

    def __init__(self):
        self.pattern = re.compile(r"^data_(\d{8}|\d{14})\.json$")

    def process(self, readable_file: fileio.ReadableFile):
        path = readable_file.metadata.path
        name = basename(path)

        # Extension check

        if self.pattern.match(name):
            yield readable_file
        else:
            yield pvalue.TaggedOutput("invalidFileName", readable_file)