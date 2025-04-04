from pydantic import BaseModel
from datetime import datetime as dt

from pandera.polars import DataFrameModel
from pandera.typing.polars import Series, DataFrame
from pandera.engines.polars_engine import DateTime
import pandera as pa
from typing import Annotated

UTCDatetime = Annotated[DateTime, False, "UTC", "ms"]


class FileStoreModel(DataFrameModel):
    file_path: Series[str] = pa.Field(unique=True)
    file_name: Series[str] = pa.Field()
    type: Series[str] = pa.Field()
    object: Series[str] = pa.Field()
    object_ra: Series[str] = pa.Field()
    object_dec: Series[str] = pa.Field()
    run_id: Series[str] = pa.Field()
    observation_id: Series[str] = pa.Field()
    time_added: Series[UTCDatetime] = pa.Field(coerce=True)
    time_creation: Series[UTCDatetime] = pa.Field(coerce=True)
    time_observation: Series[UTCDatetime] = pa.Field(coerce=True)
    exposure_seconds: Series[float] = pa.Field(nullable=True)
    dark_seconds: Series[float] = pa.Field(nullable=True)
    altitude: Series[float] = pa.Field(nullable=True)
    azimuth: Series[float] = pa.Field(nullable=True)
    cass_rotation_angle: Series[float] = pa.Field(nullable=True)
    filter: Series[str] = pa.Field(nullable=True)
    channel: Series[str] = pa.Field(nullable=True)
    detector: Series[str] = pa.Field(nullable=True)


FileStoreDataFrame = DataFrame[FileStoreModel]


class FileStoreEntry(BaseModel):
    file_path: str
    file_name: str
    type: str
    object: str
    object_ra: str
    object_dec: str
    run_id: str
    observation_id: str
    time_added: dt
    time_creation: dt
    time_observation: dt
    exposure_seconds: float | None
    dark_seconds: float | None
    altitude: float | None
    azimuth: float | None
    cass_rotation_angle: float | None
    filter: str | None
    channel: str | None
    detector: str | None


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
