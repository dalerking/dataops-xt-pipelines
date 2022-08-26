import asyncio
from contextlib import asynccontextmanager

import pyodbc
from common.utils import logger

CONN_TIMEOUT = 5
CONN_RETRIES = 5


@asynccontextmanager
async def database_connection(
    *,
    hostname: str,
    port: str,
    database: str,
    domain: str,
    username: str,
    password: str,
    driver: str,
):
    """Database connection context manager.
    Connection is disposed on exception or context leave.
    """

    conn_str = (
        f"DRIVER={driver};"
        f"SERVER={hostname};"
        f"PORT={port};"
        f"DATABASE={database};"
        # domain value for psa server is USXPRESS.COM\, must include \, and it's
        # empty str for read-replica server
        f"UID={domain}{username};"
        f"PWD={password};"
    )
    try:
        conn = await acquire_connection(conn_str)
    except pyodbc.Error as e:
        logger.exception(f"Failed to connect to database: {e}")
        raise e

    try:
        yield conn
    finally:
        conn.close()


async def acquire_connection(conn_str: str):
    """
    Connects to database and returns connection object. Retries on timeout.
    """
    for _ in range(CONN_RETRIES):
        try:
            conn_await = asyncio.get_event_loop().run_in_executor(
                executor=None,
                func=lambda: pyodbc.connect(conn_str, timeout=60, readonly=True),
            )
            return await asyncio.wait_for(conn_await, timeout=CONN_TIMEOUT)
        except asyncio.TimeoutError:
            logger.warning("Timeout error while connecting to database, retrying...")
            raise RuntimeError("Failed to connect to database, retries exceeded.")
