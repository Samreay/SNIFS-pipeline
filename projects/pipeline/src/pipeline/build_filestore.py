from pathlib import Path
from datetime import datetime as dt
from datetime import timezone as tz

from common.log import get_logger
from common.prefect_utils import pipeline_task
from pipeline.resolver.common import HEADER_MAP, FileStoreDataFrame, FileStoreModel
from pipeline.resolver.resolver import Resolver
import polars as pl
from astropy.io import fits


@pipeline_task()
def build_filestore(refresh: bool = False) -> Resolver:
    resolver = Resolver()

    logger = get_logger()
    file_store = resolver.file_store if resolver.file_store_exists() else None
    analysed_files = [] if file_store is None else file_store["file_path"].to_list()

    dfs = [] if file_store is None or refresh else [file_store]

    # If files are deleted, we want to reflect this as well.
    detected_filepaths = []
    all_files = (
        list(resolver.run_folder_path.rglob("**/*.fits"))
        + list(resolver.processing_folder_path.rglob("**/*.fits"))
        + list(resolver.processing_folder_path.rglob("**/*.parquet"))
    )
    logger.info(f"Found {len(all_files)} files in the data path. Rebuilding the filestore.")
    for file in all_files:
        relative_path = file.relative_to(resolver.data_path)
        detected_filepaths.append(str(relative_path))
        if refresh or relative_path not in analysed_files:
            dfs.append(extract_file_details(file, relative_path))

    df = (
        pl.concat(dfs, how="vertical_relaxed", rechunk=True)
        .sort("time_added")
        .unique(subset=["file_path"], keep="last", maintain_order=True)
        .filter(pl.col("file_path").is_in(detected_filepaths))
        .pipe(FileStoreDataFrame)
    )
    logger.info(f"Writing filestore with shape {df.shape} to {resolver.file_store_path}")
    df.write_parquet(resolver.file_store_path)
    # Validate the file store exists and can be loaded
    resolver.file_store
    return resolver


def extract_file_details(path: Path, relative_path: Path) -> FileStoreDataFrame:
    with fits.open(path) as hdul:  # type: ignore
        # Assume headers are in the first HDU
        header = hdul[0].header
        # Extract relevant information from the header
        values = {"file_path": str(relative_path), "file_name": path.name, "time_added": dt.now(tz=tz.utc)}
        for column in FileStoreModel.to_schema().columns:
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

        return FileStoreDataFrame(values)


if __name__ == "__main__":
    build_filestore(refresh=True)
