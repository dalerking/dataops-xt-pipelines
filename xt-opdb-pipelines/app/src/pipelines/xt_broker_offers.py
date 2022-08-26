from common.utils import logger
from src.pipelines.common.xt_data_pipeline import (
    XTDataPipeline,
    XTDataPipelineConversion,
)


class XTBrokerOffers(XTDataPipeline):
    def __init__(self, local_test: bool) -> None:
        super().__init__(
            {
                "api_index": "prod_ihaa_bid_analytics_record",
                "columns": [
                    {"name": "bid_amount"},
                    {"name": "bid_analytics_record_id"},
                    {"name": "bid_id"},
                    {"name": "broker_id"},
                    {"name": "broker_region"},
                    {"name": "broker_team"},
                    {"name": "carrier_account_type"},
                    {"name": "carrier_city"},
                    {"name": "carrier_dot_number"},
                    {"name": "carrier_id"},
                    {"name": "carrier_mc_number"},
                    {"name": "carrier_name"},
                    {"name": "carrier_state_province"},
                    {"name": "lifecycle_status"},
                    {"name": "load_drop_off_city"},
                    {"name": "load_drop_off_state"},
                    {
                        "name": "load_early_pick_up_time",
                        "conversion": XTDataPipelineConversion.TimeinMs,
                    },
                    {"name": "load_id"},
                    {
                        "name": "load_late_pick_up_time",
                        "conversion": XTDataPipelineConversion.TimeinMs,
                    },
                    {"name": "load_order_number", "replace": "order_number"},
                    {"name": "load_pick_up_city"},
                    {"name": "load_pick_up_state"},
                    {"name": "load_tmw_number", "replace": "tmw_number"},
                    {"name": "notes"},
                    {"name": "source_type"},
                    {
                        "name": "system_created",
                        "replace": "system_created_ts",
                        "conversion": XTDataPipelineConversion.TimeinMs,
                    },
                    {"name": "system_created_id"},
                    {
                        "name": "system_created_ts",
                        "conversion": XTDataPipelineConversion.TimeinMs,
                    },
                    {
                        "name": "system_updated",
                        "replace": "system_updated_ts",
                        "conversion": XTDataPipelineConversion.TimeinMs,
                    },
                    {"name": "system_updated_id"},
                    {"name": "system_updated_ts"},
                ],
                "db_table_name": "XNS_reporting.dbo.XT_BrokerOS_Offers",
            },
            local_test,
        )

    def process(self, db_connection=None) -> bool:
        try:
            cursor = None
            max_db_system_updated_ts_timestamp = None

            if db_connection is not None:
                cursor = db_connection.cursor()

                sql = f"SELECT MAX(system_updated_ts) FROM {self.db_table_name}"
                if self.local_test:
                    logger.info(f"SQL: {sql}")
                cursor.execute(sql)

                for row in cursor.fetchall():
                    if row[0] is not None:
                        max_db_system_updated_ts_timestamp = int(row[0])

                if self.local_test:
                    logger.info(f"Max: {max_db_system_updated_ts_timestamp}")

            for api_row in self.search(
                "system_updated_ts", max_db_system_updated_ts_timestamp
            ):
                self.insert_row(cursor, api_row)

            if db_connection is not None:
                db_connection.commit()

        except Exception as exception:
            self.handleException(exception)

        return self.successful
