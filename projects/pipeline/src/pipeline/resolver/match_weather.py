from pipeline.resolver.common import FileStoreDataFrame, FileStoreEntry
import polars as pl
from pipeline.resolver.registry import file_match_registry


@file_match_registry.register("WEATHER")
def find_weather_files(science_file: FileStoreEntry, file_store: FileStoreDataFrame) -> list[FileStoreEntry]:
    """
    Finds the weather file. Does not care about what the science file is right now.
    """
    files = file_store.filter((pl.col("type").eq("WEATHER")))
    return [FileStoreEntry.model_validate(row) for row in files.to_dicts()]
