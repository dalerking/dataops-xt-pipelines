import json
from functools import wraps

import boto3
import epsagon


def monitor(epsagon_func, app_name):
    def decorator(func):
        @wraps(func)
        def load_start(*args, **kwargs):
            epsagon.init(
                token=get_secret(),
                app_name=app_name,
                metadata_only=False,
            )
            return epsagon_func(func)(*args, **kwargs)

        return load_start

    return decorator


def get_secret():
    public_name = "epsagon-token"

    # Create a Secrets Manager client
    client = boto3.client("secretsmanager")
    get_secret_value_response = client.get_secret_value(SecretId=public_name)
    token = get_secret_value_response["SecretString"]
    return json.loads(token)["Epsagon Token"]
