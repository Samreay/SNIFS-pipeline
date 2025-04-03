from functools import cached_property
from typing import overload
from pipeline.config import ChannelReduction
from common.prefect_utils import pipeline_task
from pydantic import BaseModel, Field, field_validator
from pathlib import Path
import polars as pl

from pandera.polars import Column, DataFrameSchema
from pandera.engines import polars_engine

UTCDatetime = polars_engine.DateTime(time_unit="ms", time_zone="UTC")


FileStoreSchema = DataFrameSchema(
    columns={
        "file_path": Column(str),
        "file_name": Column(str),
        "type": Column(str),
        "object": Column(str),
        "object_ra": Column(str),
        "object_dec": Column(str),
        "run_id": Column(str),
        "observation_id": Column(str),
        "time_added": Column(UTCDatetime),
        "time_creation": Column(UTCDatetime),
        "time_observation": Column(UTCDatetime),
        "exposure_seconds": Column(float, nullable=True),
        "dark_seconds": Column(float, nullable=True),
        "altitude": Column(float, nullable=True),
        "azimuth": Column(float, nullable=True),
        "cass_rotation_angle": Column(float, nullable=True),
        "filter": Column(str, nullable=True),
        "channel": Column(str, nullable=True),
        "detector": Column(str, nullable=True),
    },
    strict="filter",
    coerce=True,
)

HEADER_MAP = {
    "type": "OBSTYPE",
    "run_id": "RUNID",
    "observation_id": "OBSID",
    "time_added": "TIME_ADDED",
    "time_creation": "UTC",
    "time_observation": "DATE-OBS",
    "exposure_seconds": "EXPTIME",
    "dark_seconds": "DARKTIME",
    "cass_rotation_angle": "ROTANGLE",
    "object_ra": "OBJRA",
    "object_dec": "OBJDEC",
}


class Resolver(BaseModel):
    # TODO: discuss how cloud focused this should be. Ideally this resolve had both local pref and cloud fetching built in.
    file_store_path: Path = Field(default_factory=lambda: Path(__file__).parents[5] / "data/store.parquet")
    run_data_path: Path = Field(default_factory=lambda: Path(__file__).parents[5] / "data/runs")

    @field_validator("file_store_path")
    @classmethod
    def check_file_store_path(cls, v: str | Path) -> Path:
        if isinstance(v, str):
            return Path(v)
        return v

    @cached_property
    def file_store(self) -> pl.DataFrame | None:
        if not self.file_store_path.exists():
            return None
        df: pl.DataFrame = pl.read_parquet(self.file_store_path).pipe(FileStoreSchema.validate)  # type: ignore
        return df
