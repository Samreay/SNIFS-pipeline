from common.prefect_utils import pipeline_flow
from common.log import get_logger
from pipeline.config import ChannelReduction
from pipeline.resolver import Resolver


@pipeline_flow()
def reduce_channel(initial_config: ChannelReduction) -> None:
    logger = get_logger()
    logger.info(f"Starting channel exposure reduction with settings:\n {initial_config.model_dump_json(indent=2)}")
    logger.info("Running the config through the resolver to fill all unspecified inputs")

    resolver = Resolver()

    # logger.info(f"Final config:\n {final_config.model_dump_json(indent=2)}")


if __name__ == "__main__":
    config = ChannelReduction()
    reduce_channel(config)
