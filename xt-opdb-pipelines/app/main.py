import asyncio
import json

import epsagon
from common.constants import SERVICE_NAME
from common.epsagon_init import monitor as epsagon_monitor
from common.utils import logger, metrics, tracer
from src.pipeline import run_pipeline, test_pipelines


@metrics.log_metrics(capture_cold_start_metric=True)
@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
@epsagon_monitor(epsagon.lambda_wrapper, app_name=SERVICE_NAME)
async def main_handler(event, context):
    logger.info("Received event: " + json.dumps(event, indent=2))
    table_length = await run_pipeline(event=event, context=context)
    logger.info("Lambda function for script finished with SUCCESS")

    return {"rowCount": table_length}


def lambda_handler(event, context):
    return asyncio.run(main_handler(event, context))


if __name__ == "__main__":
    test_pipelines()
