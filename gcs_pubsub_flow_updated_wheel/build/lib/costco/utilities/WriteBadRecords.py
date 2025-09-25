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

        # ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")
        # out_name = f"{self.prefix}/today/{source_file}_{ts}.json"

        out_name = self.prefix  +  str(datetime.now().strftime("%Y-%m-%d")) + "/" + source_file +"_" + str(datetime.now().strftime("%Y%m%dT%H%M%S%fZ")) + ".json"

        line = json.dumps(
            {"source_path": path, "reason": reason, "payload": obj},
            ensure_ascii=False,
        ) + "\n"

        try:
            # Using `FileSystems.create` in 'append' mode if supported, otherwise Beam will create multiple files
            with FileSystems.create(out_name) as f:
                f.write(line.encode("utf-8"))
            # Optional: logging
            # logging.info("Wrote invalid record to %s", out_name)
        except Exception as e:
            logging.exception("Failed writing bad record for %s: %s", path, e)

# class WriteBadRecordDoFn(beam.DoFn):
#     """
#     Writes bad records as NDJSON lines into GCS under bad_records/invalid_data/.
#     Each element results in one line appended to a sharded object.
#     For simplicity in streaming, we write one object per element (safe but more files).
#     """
#     def __init__(self, prefix: str):
#         self.prefix = str(prefix).rstrip("/") + "/"

#     def process(self, item):
#         # item can be (path, raw_text, error) or (path, rec_dict, reason)
#         ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")
#         out_name = f"{self.prefix}/invalid_{ts}.json"
#         try:
#             if isinstance(item, tuple) and len(item) == 3:
#                 path, obj, reason = item
#             else:
#                 path, obj, reason = ("", item, "unknown")
#             line = json.dumps(
#                 {"source_path": path, "reason": reason, "payload": obj},
#                 ensure_ascii=False,
#             )
#             with FileSystems.create(out_name) as f:
#                 f.write(line.encode("utf-8"))
#                 f.write(b"\n")
#             logging.info("Wrote invalid record to %s", out_name)
#         except Exception as e:
#             logging.exception("Failed writing bad record: %s", e)