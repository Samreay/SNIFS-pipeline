from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    data_path: Path = Field(default_factory=lambda: Path(__file__).parents[5] / "data")
    output_path: Path = Field(default_factory=lambda: Path(__file__).parents[5] / "output")


settings = Settings()
