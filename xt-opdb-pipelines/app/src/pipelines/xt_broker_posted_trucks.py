from datetime import datetime
from src.pipelines.common.xt_data_pipeline import (
    XTDataPipeline,
    XTDataPipelineConversion,
)

import re


class XTBrokerPostedTrucks(XTDataPipeline):
    def __init__(self, search_size=10000) -> None:
        super().__init__(
            {
                "api_index": "prod_ihra_postedtruck_broker_reporting",
                "columns": [
                    {"name": "active", "conversion": XTDataPipelineConversion.Flag},
                    {
                        "name": "availability",
                        "conversion": XTDataPipelineConversion.TimeinMs,
                    },
                    {"name": "broker_id"},
                    {"name": "broker_name"},
                    {"name": "broker_office"},
                    {"name": "broker_team"},
                    {"name": "carrier_dot"},
                    {"name": "creation_type"},
                    {"name": "destination"},
                    {
                        "name": "destination_geo_point",
                        "conversion": XTDataPipelineConversion.GeoPoint,
                    },
                    {"name": "destination_radius"},
                    {"name": "destination_timezone"},
                    {"name": "destination_zip"},
                    {"name": "equipment_type"},
                    {"name": "hazmat"},
                    {"name": "id"},
                    {
                        "name": "last_updated_at",
                        "conversion": XTDataPipelineConversion.TimeinMs,
                    },
                    {"name": "origin"},
                    {
                        "name": "origin_geo_point",
                        "conversion": XTDataPipelineConversion.GeoPoint,
                    },
                    {"name": "origin_radius"},
                    {"name": "origin_timezone"},
                    {"name": "origin_zip"},
                    {
                        "name": "posted_at",
                        "conversion": XTDataPipelineConversion.TimeinMs,
                    },
                    {"name": "conversion"},
                    {"name": "user_id"},
                ],
                "db_table_name": "XNS_reporting.dbo.XT_BrokerOS_PostedTruck",
            },
            search_size,
        )

    def process(self, db_connection=None) -> bool:
        try:
            cursor = None
            max_db_posted_at_timestamp = None

            if db_connection is not None:
                cursor = db_connection.cursor()

                sql = f"SELECT MAX(posted_at) FROM {self.db_table_name}"
                cursor.execute(sql)

                for row in cursor.fetchall():
                    max_posted_at1 = str(row[0])

                    if max_posted_at1 is not None and max_posted_at1 != "None":
                        max_posted_at2 = re.sub(
                            "-", ":", re.sub(" ", ":", max_posted_at1)
                        )
                        max_posted_at3 = max_posted_at2.split(":")
                        max_db_posted_at_timestamp = int(
                            round(
                                datetime(
                                    int(max_posted_at3[0]),
                                    int(max_posted_at3[1]),
                                    int(max_posted_at3[2]),
                                    int(max_posted_at3[3]),
                                    int(max_posted_at3[4]),
                                    int(max_posted_at3[5]),
                                ).timestamp()
                                * 1000
                            )
                        )

            for api_row in self.search("posted_at", max_db_posted_at_timestamp):
                self.insert_row(cursor, api_row)

            if db_connection is not None:
                db_connection.commit()

        except Exception as exception:
            self.handleException(exception)

        return self.successful
