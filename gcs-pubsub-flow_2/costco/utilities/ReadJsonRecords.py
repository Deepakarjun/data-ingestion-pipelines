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


class ReadJsonRecordsDoFn(beam.DoFn):
    """
    Reads a ReadableFile and emits (file_path, record_dict) for each valid JSON record.
    It supports:
      - single JSON object => 1 record
      - JSON array => N records
      - JSON Lines (one JSON per line)
    Invalid JSON lines/blocks are emitted to tagged output 'invalid' as (file_path, raw_text, error_msg).
    """
    def process(self, readable_file: fileio.ReadableFile):
        path = readable_file.metadata.path
        try:
            # Try full-file JSON first
            data = readable_file.read_utf8()
            try:
                parsed = json.loads(data)
                if isinstance(parsed, dict):
                    yield (path, parsed)
                elif isinstance(parsed, list):
                    for rec in parsed:
                        if isinstance(rec, dict):
                            yield (path, rec)
                        else:
                            yield TaggedOutput("invalid", (path, json.dumps(rec, ensure_ascii=False), "array item not object"))
                else:
                    # Fallback to JSONL attempt below
                    raise ValueError("Top-level JSON is not dict or list")
            except Exception:
                # Try JSON Lines
                for i, line in enumerate(data.splitlines(), start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                        if isinstance(rec, dict):
                            yield (path, rec)
                        else:
                            yield TaggedOutput("invalid", (path, line, f"line {i}: not a JSON object"))
                    except Exception as je:
                        yield TaggedOutput("invalid", (path, line, f"line {i}: {je}"))
        except Exception as e:
            yield TaggedOutput("invalid", (path, "", f"read failure: {e}"))