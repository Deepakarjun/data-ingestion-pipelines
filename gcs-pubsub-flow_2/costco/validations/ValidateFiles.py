import apache_beam as beam
from apache_beam import DoFn
from apache_beam.io import fileio
import re
from os.path import basename
from apache_beam import pvalue

class ValidateFileDoFn(beam.DoFn):
    """
    Validates file by:
      - extension must be .json
      - filename must match data_YYYYMMDD.json
      - and the date must equal expected_date (if provided)
    Routes to:
      - main: valid files (ReadableFile)
      - 'bad_filename': bad name/date
      - 'bad_extension': other extension
    """
    def __init__(self, expected_date_yyyymmdd: str | None):
        self.expected_date = expected_date_yyyymmdd
        self.pattern = re.compile(r"^data_(\d{8})\.json$")

    def process(self, readable_file: fileio.ReadableFile):
        path = readable_file.metadata.path
        name = basename(path)

        # Extension check
        if not name.endswith(".json"):
            yield pvalue.TaggedOutput("bad_filename", readable_file)
            return

        # Filename pattern check
        m = self.pattern.match(name)
        if not m:
            yield pvalue.TaggedOutput("bad_filename", readable_file)
            return

        if self.expected_date:
            file_date = m.group(1)
            if file_date != self.expected_date:
                yield pvalue.TaggedOutput("bad_filename", readable_file)
                return

        # Valid
        yield readable_file