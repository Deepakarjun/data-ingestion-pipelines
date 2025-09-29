import apache_beam as beam
from apache_beam import DoFn
from apache_beam.io import fileio
import re
from os.path import basename
from apache_beam import pvalue

class ValidateFileDoFn(beam.DoFn):

    def __init__(self, file_name_pattern):
        self.pattern = re.compile(file_name_pattern)

    def process(self, readable_file: fileio.ReadableFile):
        path = readable_file.metadata.path
        name = str(basename(path)).split(".")[0]

        if self.pattern.match(name):
            yield readable_file
        else:
            yield pvalue.TaggedOutput("invalidFileNamePattern", readable_file)