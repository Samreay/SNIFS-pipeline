from pipeline.resolver.resolver import Resolver
from pydantic import Field, FilePath
from pydantic_settings import BaseSettings
from datetime import datetime as dt


# TODO: I dont like any of these being FilePaths, but figured we'll start here
class ChannelReduction(BaseSettings):
    science_file: FilePath = Field(
        description="Location of the science file. This is actual observation. Relative to the data path."
    )

    arc_file: str = Field(
        default="",
        description="Location of the arc file(s). For a single exposure, the arc is usually taken immediately after the science exposure."
        "For two exposures, the arc is usually in the middle.",
    )

    weather_file: str = Field(default="", description="Location of the weather file")

    continuum_files: list[str] = Field(
        default=[],
        description="Location of the continuum file. "
        "These are lamp exposures used to monitor shifts versus wavelength in the dichroic throughput."
        "For example, humidity changes the thickness of the multilayer interface coating"
        "Sometimes these images are referred to as 'raster' images or 'flats'. "
        "We normally have multiple continuum files per night. "
        "Five at the start, one in the middle of the night, and five more in the morning. "
        "These are often also called flats. "
        "And an image from a central CCD is often called a 'raster'.",
    )

    detector_last_on_time: dt | None = Field(
        default=None,
        description="The time the detector was last on (ie when the moment it was last turned off)."
        "This is needed because the amount of time the instrument has been idle impacts readings.",
    )

    use_cache: bool = Field(default=True, description="Use cached data when possible")

    def resolve_missing(self, resolver: Resolver) -> None:
        """
        Resolves the paths for the channel reduction config.
        This is a bit of a hack, but it works for now.
        """
        primary = resolver.get_file_metadata(self.science_file)
        if not self.arc_file:
            self.arc_file = resolver.get_match_path("ARC", primary)
        if not self.continuum_files:
            self.continuum_files = resolver.get_match_paths("FLAT", primary)
        if not self.weather_file:
            self.weather_file = resolver.get_match_path("WEATHER", primary)
