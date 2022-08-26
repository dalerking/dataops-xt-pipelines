import json

import boto3
from common.constants import NAMESPACE_NAME, SERVICE_NAME
from aws_lambda_powertools import Logger, Metrics, Tracer

logger = Logger(service=SERVICE_NAME)
tracer = Tracer()
metrics = Metrics(namespace=NAMESPACE_NAME, service=SERVICE_NAME)

SECRET_RETRIES = 5


def get_secret(key_name: str) -> dict:
    logger.info(f"Fetching secret from secrets manager {key_name}")
    client = boto3.client("secretsmanager")

    for _ in range(SECRET_RETRIES):
        response = client.get_secret_value(SecretId=key_name)
        return json.loads(response["SecretString"])

    raise RuntimeError("Failed to connect to secretsmanager, retries exceeded.")
