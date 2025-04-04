from functools import cached_property
from pipeline.resolver.common import FileStoreEntry, FileStoreDataFrame
from pydantic import BaseModel, Field, field_validator
from pathlib import Path
import polars as pl
from pipeline.resolver import file_match_registry


class Resolver(BaseModel):
    # TODO: discuss how cloud focused this should be. Ideally this resolve had both local pref and cloud fetching built in.
    file_store_path: Path = Field(default_factory=lambda: Path(__file__).parents[5] / "data/store.parquet")

    data_path: Path = Field(default_factory=lambda: Path(__file__).parents[5] / "data")
    run_folder: str = Field(default="runs")
    processing_folder: str = Field(default="processing")

    @property
    def run_folder_path(self) -> Path:
        return self.data_path / self.run_folder

    @property
    def processing_folder_path(self) -> Path:
        return self.data_path / self.processing_folder

    @field_validator("file_store_path")
    @classmethod
    def check_file_store_path(cls, v: str | Path) -> Path:
        if isinstance(v, str):
            return Path(v)
        return v

    def file_store_exists(self) -> bool:
        return self.file_store_path.exists()

    @cached_property
    def file_store(self) -> FileStoreDataFrame:
        assert self.file_store_path.exists(), f"File store not found at {self.file_store_path}."
        return pl.read_parquet(self.file_store_path).pipe(FileStoreDataFrame)

    def get_file_metadata(self, file_path: Path) -> FileStoreEntry:
        """
        Get the metadata for a file.
        """
        if self.file_store is None:
            raise FileNotFoundError(f"File store not found at {self.file_store_path}.")
        relative_path = str(file_path.relative_to(self.data_path))
        file_records = self.file_store.filter(pl.col("file_path").eq(relative_path))
        if len(file_records) == 0:
            raise FileNotFoundError(f"File {relative_path} not found in file store.")
        assert len(file_records) == 1, f"Found multiple records for {relative_path}"
        return FileStoreEntry.model_validate(file_records.to_dicts()[0])

    def get_match(
        self,
        file_type: str,
        primary: FileStoreEntry,
    ) -> FileStoreEntry:
        """
        Get a single match for a file type.
        """
        return file_match_registry.get_match(file_type, primary, self.file_store)

    def get_match_path(
        self,
        file_type: str,
        primary: FileStoreEntry,
    ) -> str:
        return self.get_match(file_type, primary).file_path

    def get_matches(
        self,
        file_type: str,
        primary: FileStoreEntry,
    ) -> list[FileStoreEntry]:
        """
        Get all matches for a file type.
        """
        return file_match_registry.get_matches(file_type, primary, self.file_store)

    def get_match_paths(
        self,
        file_type: str,
        primary: FileStoreEntry,
    ) -> list[str]:
        """
        Get all match paths for a file type.
        """
        return [match.file_path for match in self.get_matches(file_type, primary)]
