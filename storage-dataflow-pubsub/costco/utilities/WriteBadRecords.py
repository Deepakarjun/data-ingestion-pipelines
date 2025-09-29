import apache_beam as beam
from apache_beam import DoFn
import json
from apache_beam.io.filesystems import FileSystems
from datetime import datetime
from os.path import basename


class WriteBadRecordDoFn(beam.DoFn):

    def __init__(self, prefix: str):
        self.prefix = str(prefix).rstrip("/") + "/"

    def process(self, item):

        path, obj, reason = item
        source_file = basename(path).split(".")[0]

        out_name = self.prefix  +  str(datetime.now().strftime("%Y-%m-%d")) + "/" + source_file +"_" + str(datetime.now().strftime("%Y%m%dT%H%M%S%fZ")) + ".json"

        line = json.dumps(
            {"source_path": path, "reason": reason, "payload": obj},
            ensure_ascii=False,
        ) + "\n"

        try:
            with FileSystems.create(out_name) as f:
                f.write(line.encode("utf-8"))
        except Exception as e:
            logging.exception("Failed writing bad record for %s: %s", path, e)
