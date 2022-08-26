from common.utils import logger
from src.pipelines.common.xt_data_pipeline import XTDataPipeline


class XTBrokerLoadAssignment(XTDataPipeline):
    def __init__(self, local_test: bool) -> None:
        super().__init__(
            {
                "api_index": "prod_ihta_load_broker_assignment_reporting",
                "columns": [
                    {"name": "Brokers", "combine": "broker_id", "json_path": "brokers"},
                    {"name": "LoadId", "json_path": "load_id"},
                    {"name": "OrderNumber", "json_path": "order_number"},
                    {"name": "TMWNumber", "json_path": "tmw_number"},
                    {"name": "UpdatedAt", "json_path": "updated_at"},
                    {"name": "UpdatedBy", "json_path": "updated_by"},
                ],
                "db_table_name": "XNS_reporting.dbo.XT_BrokerOS_Load_Assignment",
            },
            local_test,
        )

    def process(self, db_connection=None) -> bool:
        try:
            cursor = None
            max_updated_at_timestamp = 0

            if db_connection is not None:
                cursor = db_connection.cursor()

                sql = f"SELECT MAX(UpdatedAt) FROM {self.db_table_name}"
                if self.local_test:
                    logger.info(f"SQL: {sql}")
                cursor.execute(sql)

                for row in cursor.fetchall():
                    if row[0] is not None:
                        max_updated_at_timestamp = int(row[0])

                if self.local_test:
                    logger.info(f"Max: {max_updated_at_timestamp}")

            for api_row in self.search("updated_at", max_updated_at_timestamp):
                self.insert_row(cursor, api_row)

            if db_connection is not None:
                db_connection.commit()

        except Exception as exception:
            self.handleException(exception)

        return self.successful
