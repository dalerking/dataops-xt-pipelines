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

import asyncio, logging


def set_parameters(event: dict, local_test: bool) -> dict:
    parsed_event = Event.parse_obj(event)

    database_details = get_secret(parsed_event.data.databaseDetails["secretManager"])
    logger.info("Database credentials from secretsmanager loaded")

    if local_test:
        database_details["db_driver"] = "{ODBC Driver 17 for SQL Server}"

    return {
        "database_details": database_details,
        "pipeline_name": parsed_event.data.jobDetails.pipelineName,
    }


async def run_pipeline(event, context, local_test=False):
    pipeline = None

    try:
        parameters = set_parameters(event, local_test)

        pipeline_name = parameters["pipeline_name"]
        logger.info(f"Process started for {pipeline_name}.")

        if pipeline_name == "xt-broker-driver-info":
            pipeline = XTBrokerDriverInfo(local_test)
        elif pipeline_name == "xt-broker-load-assignment":
            pipeline = XTBrokerLoadAssignment(local_test)
        elif pipeline_name == "xt-broker-offers":
            pipeline = XTBrokerOffers(local_test)
        elif pipeline_name == "xt-broker-posted-trucks":
            pipeline = XTBrokerPostedTrucks(local_test)
        elif pipeline_name == "xt-ml-recommendations":
            pipeline = XTMLRecommendations(local_test)
        elif pipeline_name == "xt-product-amplitude":
            pipeline = XTProductAmplitude(local_test)
        elif pipeline_name == "xt-rolled-reason":
            pipeline = XTRolledReason(local_test)

        if pipeline is None:
            raise USXException(f"Pipeline not supported ({pipeline_name})")
        else:
            async with database_connection(
                **load_database_env_values(parameters["database_details"], local_test)
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    pipeline_name = "xt-broker-driver-info"
    # pipeline_name = "xt-broker-load-assignment"
    # pipeline_name = "xt-broker-offers"
    # pipeline_name = "xt-broker-posted-trucks"
    # pipeline_name = "xt-ml-recommendations"
    # pipeline_name = "xt-product-amplitude"
    # pipeline_name = "xt-rolled-reason"

    event = {
        "data": {
            "databaseDetails": {
                "dbDriver": "libtdsodbc.so",
                "secretManager": "xt_database_creds",
            },
            "jobDetails": {
                "pipelineName": pipeline_name,
                "secretManager": "xt-elasticsearch-creds",
            },
        }
    }

    asyncio.run(run_pipeline(event, None, True))
