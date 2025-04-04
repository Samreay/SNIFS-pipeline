from pathlib import Path
from common.prefect_utils import pipeline_flow
from common.log import get_logger
from pipeline.build_filestore import build_filestore
from pipeline.config import ChannelReduction
from pipeline.tasks import preprocess, augment_science_file, correct_dichoric, remove_continuum, calibrate_with_flats


@pipeline_flow()
def reduce_channel_exposure(config: ChannelReduction) -> None:
    logger = get_logger()
    logger.info(f"Starting channel exposure reduction with settings:\n {config.model_dump_json(indent=2)}")
    logger.info("Running the config through the resolver to fill all unspecified inputs")

    resolver = build_filestore()
    config.resolve_missing(resolver)
    logger.info(f"Final config:\n {config.model_dump_json(indent=2)}")

    preprocess()
    augment_science_file()
    correct_dichoric()
    remove_continuum()
    calibrate_with_flats()


if __name__ == "__main__":
    # TODO: allow string and relative dir validation
    science_file = Path(__file__).parents[4] / "data/runs/run_id=25_056_084/science_blue.fits"
    print(str(science_file))
    config = ChannelReduction(science_file=science_file)
    reduce_channel_exposure(config)
