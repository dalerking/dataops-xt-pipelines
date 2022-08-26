from src.pipelines.common.xt_data_pipeline import (
    XTDataPipeline,
    XTDataPipelineConversion,
)

import re


class XTMLRecommendations(XTDataPipeline):
    def __init__(self, search_size=10000) -> None:
        super().__init__(
            {
                "api_index": "prod_ihrz_recommendations",
                "columns": [
                    {"name": "broker_id", "json_path": "payload.broker_id"},
                    {"name": "carrier_dot", "json_path": "payload.carrier_dot"},
                    {
                        "name": "event_time",
                        "json_path": "meta.event_time",
                        "conversion": XTDataPipelineConversion.EventTime,
                    },
                    {
                        "name": "explanationtype",
                        "json_path": "payload.explanation.type",
                    },
                    {"name": "lane_id", "json_path": "payload.lane_id"},
                    {"name": "load_id", "json_path": "payload.load_id"},
                    {"name": "match_percent", "json_path": "payload.match_percent"},
                    {"name": "order_number", "json_path": "payload.order_number"},
                    {
                        "name": "recommendation_id",
                        "json_path": "payload.recommendation_id",
                    },
                    {
                        "name": "similar_load_id",
                        "json_path": "payload.explanation.similar_load_id",
                    },
                    {"name": "truck_id", "json_path": "payload.truck_id"},
                    {"name": "conversion", "json_path": "payload.type"},
                ],
                "db_table_name": "XNS_reporting.dbo.XT_ML_Recommendations",
            },
            search_size,
        )

    def process(self, db_connection=None) -> bool:
        try:
            cursor = None
            max_db_event_time_timestamp = None

            if db_connection is not None:
                cursor = db_connection.cursor()

                sql = f"SELECT MAX(event_time) FROM {self.db_table_name}"
                cursor.execute(sql)

                for row in cursor.fetchall():
                    max_db_event_time_timestamp_1 = str(row[0])

                    if (
                        max_db_event_time_timestamp_1 is not None
                        and max_db_event_time_timestamp_1 != "None"
                    ):
                        max_db_event_time_timestamp = re.sub(
                            "$",
                            ".999999+00:00",
                            max_db_event_time_timestamp_1.replace(" ", "T"),
                        )

            for api_row in self.search("meta.event_time", max_db_event_time_timestamp):
                self.insert_row(cursor, api_row)

            if db_connection is not None:
                db_connection.commit()

        except Exception as exception:
            self.handleException(exception)

        return self.successful
