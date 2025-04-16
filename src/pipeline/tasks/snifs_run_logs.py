from pipeline.common.prefect_utils import pipeline_task
from pipeline.resolver.resolver import Resolver
from pipeline.resolver.common import DATETIME_CONVERSION_EXPR, FileType
import re
from datetime import datetime as dt
import polars as pl


@pipeline_task()
def parse_snifs_run_logs(resolver: Resolver):
    raw_logs_path = resolver.get_match_path(FileType.RAW_LOGS)
    regex = re.compile(r"SNIFS_on \d \d (.) ==>(.*)\s+$")
    lines = raw_logs_path.read_text(encoding="UTF-8", errors="replace").splitlines()
    lines = [line for line in lines if "SNIFS_on" in line]
    results = []
    for line in lines:
        if match := regex.search(line):
            channel, time_str = match.group(1), match.group(2)
            results.append(dict(channel=channel, time=dt.strptime(time_str, "%a %b %d %H:%M:%S %Z %Y")))

    output_path = resolver.processed_data_path / f"type={FileType.CCD_ON_TIMES.value}" / "times.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(results).sort("time").with_columns(DATETIME_CONVERSION_EXPR)
    df.write_parquet(output_path, compression="gzip")
    resolver.ensure_file_exists(output_path)
