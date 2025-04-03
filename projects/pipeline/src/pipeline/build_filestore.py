from pathlib import Path
from datetime import datetime as dt
from datetime import timezone as tz

from common.prefect_utils import pipeline_task
from pipeline.resolver.resolver import Resolver, FileStoreSchema, HEADER_MAP
import polars as pl
from astropy.io import fits


@pipeline_task()
def build_filestore(refresh: bool = False) -> pl.DataFrame:
    resolver = Resolver()

    file_store = resolver.file_store
    analysed_files = [] if file_store is None else file_store["file_path"].to_list()

    dfs = [] if file_store is None or refresh else [file_store]
    for file in resolver.run_data_path.rglob("**/*.fits"):
        relative_path = file.relative_to(resolver.run_data_path)
        if refresh or relative_path not in analysed_files:
            dfs.append(extract_file_details(file, relative_path))

    df: pl.DataFrame = (
        pl.concat(dfs, how="vertical_relaxed", rechunk=True)
        .sort("time_added")
        .unique(subset=["file_path"], keep="last", maintain_order=True)
        .pipe(FileStoreSchema.validate)
    )  # type: ignore
    df.write_parquet(resolver.file_store_path)
    return df


@pipeline_task()
def extract_file_details(path: Path, relative_path: Path) -> pl.DataFrame:
    with fits.open(path) as hdul:  # type: ignore
        # Assume headers are in the first HDU
        header = hdul[0].header
        # Extract relevant information from the header
        values = {"file_path": str(relative_path), "file_name": path.name, "time_added": dt.now(tz=tz.utc)}
        for column in FileStoreSchema.columns:
            expected_column_name = HEADER_MAP.get(column, column)
            if expected_column_name in header:
                value = header[expected_column_name]
                if isinstance(value, str):
                    value = value.strip()
                if column.startswith("time"):
                    if isinstance(value, int):
                        value = dt.fromtimestamp(value, tz=tz.utc)
                    elif isinstance(value, str):
                        value = dt.strptime(value, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=tz.utc)
                values[column] = value

        return pl.DataFrame(values).pipe(FileStoreSchema.validate)  # type: ignore


if __name__ == "__main__":
    build_filestore(refresh=True)
