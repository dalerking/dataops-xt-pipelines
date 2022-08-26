from common.utils import get_secret, logger
from data_model import Event
from src.utilities.create_connection import database_connection
from src.utilities.database_utilities import (
    load_database_env_values,
)

from src.pipelines.common.usx_exception import USXException
from src.pipelines.xt_broker_driver_info import XTBrokerDriverInfo
from src.pipelines.xt_broker_load_assignment import XTBrokerLoadAssignment
from src.pipelines.xt_broker_offers import XTBrokerOffers
from src.pipelines.xt_broker_posted_trucks import XTBrokerPostedTrucks
from src.pipelines.xt_ml_recommendations import XTMLRecommendations
from src.pipelines.xt_product_amplitude import XTProductAmplitude
from src.pipelines.xt_rolled_reason import XTRolledReason


def set_parameters(event: dict) -> dict:
    parsed_event = Event.parse_obj(event)

    database_details = get_secret(parsed_event.data.databaseDetails["secretManager"])
    logger.info("Database credentials from secretsmanager loaded")

    dict_settings = {
        "database_details": database_details,
        "pipeline_name": parsed_event.data.jobDetails.pipelineName,
    }

    return dict_settings


async def run_pipeline(event, context):
    pipeline = None

    try:
        dict_settings = set_parameters(event)

        pipeline_name = dict_settings["pipeline_name"]
        logger.info(f"Process started for {pipeline_name}.")

        if pipeline_name == "xt-broker-driver-info":
            pipeline = XTBrokerDriverInfo()
        elif pipeline_name == "xt-broker-load-assignment":
            pipeline = XTBrokerLoadAssignment()
        elif pipeline_name == "xt-broker-offers":
            pipeline = XTBrokerOffers()
        elif pipeline_name == "xt-broker-posted-trucks":
            pipeline = XTBrokerPostedTrucks()
        elif pipeline_name == "xt-ml-recommendations":
            pipeline = XTMLRecommendations()
        elif pipeline_name == "xt-product-amplitude":
            pipeline = XTProductAmplitude()
        elif pipeline_name == "xt-rolled-reason":
            pipeline = XTRolledReason()

        if pipeline is None:
            raise USXException(f"Pipeline not supported ({pipeline_name})")
        else:
            async with database_connection(
                **load_database_env_values(dict_settings["database_details"])
            ) as db_connection:
                pipeline.process(db_connection)
    except USXException as e:
        logger.exception(
            f"An error occurred while executing pipeline: {e.get_message()}"
        )
        raise e
    except Exception as e:
        logger.exception(f"An error occurred while executing pipeline: {e}")
        raise e


def test_pipelines():
    search_size = 1

    try:
        XTBrokerDriverInfo(search_size).process()
        XTBrokerLoadAssignment(search_size).process()
        XTBrokerOffers(search_size).process()
        XTBrokerPostedTrucks(search_size).process()
        XTMLRecommendations(search_size).process()
        XTProductAmplitude(search_size).process()
        XTRolledReason(search_size).process()
    except USXException as exception:
        print()
        print(f"  {exception.get_message()}")
