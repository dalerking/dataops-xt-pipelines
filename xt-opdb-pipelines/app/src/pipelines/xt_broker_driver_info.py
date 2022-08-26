from common.utils import logger
from datetime import datetime
from src.pipelines.common.xt_data_pipeline import (
    XTDataPipeline,
    XTDataPipelineConversion,
)

import re


class XTBrokerDriverInfo(XTDataPipeline):
    def __init__(self, local_test: bool) -> None:
        super().__init__(
            {
                "api_index": "prod_ihta_load_update_reporting",
                "columns": [
                    {"name": "broker_id"},
                    {"name": "driver_name"},
                    {"name": "driver_phone"},
                    {"name": "load_id"},
                    {"name": "order_number"},
                    {"name": "tmw_number"},
                    {"name": "tracking_type"},
                    {"name": "trailer_number"},
                    {"name": "truck_number"},
                    {
                        "name": "updated_at",
                        "conversion": XTDataPipelineConversion.TimeinMs,
                    },
                    {"name": "updated_by"},
                ],
                "db_table_name": "XNS_reporting.dbo.XT_BrokerOS_DriverInfo",
            },
            local_test,
        )

    def process(self, db_connection=None) -> bool:
        try:
            cursor = None
            max_db_updated_at_timestamp = None

            if db_connection is not None:
                cursor = db_connection.cursor()

                sql = f"SELECT MAX(updated_at) FROM {self.db_table_name}"
                if self.local_test:
                    logger.info(f"SQL: {sql}")
                cursor.execute(sql)

                for row in cursor.fetchall():
                    max_updated_at1 = str(row[0])

                    if max_updated_at1 is not None and max_updated_at1 != "None":
                        max_updated_at2 = re.sub(
                            "-", ":", re.sub(" ", ":", max_updated_at1)
                        )
                        max_updated_at3 = max_updated_at2.split(":")
                        max_db_updated_at_timestamp = int(
                            round(
                                datetime(
                                    int(max_updated_at3[0]),
                                    int(max_updated_at3[1]),
                                    int(max_updated_at3[2]),
                                    int(max_updated_at3[3]),
                                    int(max_updated_at3[4]),
                                    int(max_updated_at3[5]),
                                ).timestamp()
                                * 1000
                            )
                        )

                if self.local_test:
                    logger.info(f"Max: {max_db_updated_at_timestamp}")

            for api_row in self.search("updated_at", max_db_updated_at_timestamp):
                self.insert_row(cursor, api_row)

            if db_connection is not None:
                db_connection.commit()

        except Exception as exception:
            self.handleException(exception)

        return self.successful
