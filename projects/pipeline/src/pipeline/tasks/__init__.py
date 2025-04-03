from pipeline.tasks.augment import augment_science_file
from pipeline.tasks.calibrate import calibrate_with_flats
from pipeline.tasks.preprocess import preprocess
from pipeline.tasks.correct_dichroic import correct_dichoric
from pipeline.tasks.remove_continuum import remove_continuum

__all__ = [
    "augment_science_file",
    "calibrate_with_flats",
    "preprocess",
    "correct_dichoric",
    "remove_continuum",
]
