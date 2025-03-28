from common import data_flow, get_logger
from pydantic import Field, FilePath
from pydantic_settings import BaseSettings


# TODO: I dont like any of these being FilePaths, but figured we'll start here and
# TODO: I'll swap to DI down the line. I'm unsure if I should roll my own DI from
# TODO: a config file like this, or use dependency-injector
class PipelineSettings(BaseSettings):
    # TODO: Probably want this as FileURL so we can remotely fetch the file
    # TODO: are the initial observations for red and blue channels saved out to separate files?
    # TODO: remove the | None here when I get example files from Daniel
    science_file: FilePath | None = Field(default=None, description="Location of the science file")

    # TODO: Need to define what a raster file is again
    raster_file: FilePath | None = Field(default=None, description="Location of the raster file")

    # TODO: are the same arcs always used? Are then an arbitrary amount of arcs?
    arc_file: FilePath | None = Field(default=None, description="Location of the arc file")

    weather_file: FilePath | None = Field(default=None, description="Location of the weather file")

    # TODO: are these first class outputs from each observing run, a shared file, or something derived from the science file?
    continuum_file: FilePath | None = Field(default=None, description="Location of the continuum file")

    use_cache: bool = Field(default=True, description="Use cached data when possible")


@data_flow()
def reduce_observation(settings: PipelineSettings) -> None:
    logger = get_logger()
    logger.info("Starting reduce_observation flow")
    logger.info(f"Settings: {settings.model_dump_json(indent=2)}")


if __name__ == "__main__":
    settings = PipelineSettings()
    reduce_observation(settings)
