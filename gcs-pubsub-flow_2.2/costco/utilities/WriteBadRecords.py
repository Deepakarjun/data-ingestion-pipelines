import apache_beam as beam
from apache_beam import DoFn

class WriteBadRecordDoFn(beam.DoFn):
    """
    Writes bad records as NDJSON lines into GCS under bad_records/invalid_data/.
    Each element results in one line appended to a sharded object.
    For simplicity in streaming, we write one object per element (safe but more files).
    """
    def __init__(self, prefix: str):
        self.prefix = str(prefix).rstrip("/") + "/"

    def process(self, item):
        # item can be (path, raw_text, error) or (path, rec_dict, reason)
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")
        out_name = f"{self.prefix}/invalid_{ts}.json"
        try:
            if isinstance(item, tuple) and len(item) == 3:
                path, obj, reason = item
            else:
                path, obj, reason = ("", item, "unknown")
            line = json.dumps(
                {"source_path": path, "reason": reason, "payload": obj},
                ensure_ascii=False,
            )
            with FileSystems.create(out_name) as f:
                f.write(line.encode("utf-8"))
                f.write(b"\n")
            logging.info("Wrote invalid record to %s", out_name)
        except Exception as e:
            logging.exception("Failed writing bad record: %s", e)