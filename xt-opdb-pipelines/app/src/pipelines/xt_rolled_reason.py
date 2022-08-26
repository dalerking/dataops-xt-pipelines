from common.utils import logger
from src.pipelines.common.xt_data_pipeline import (
    XTDataPipeline,
    XTDataPipelineConversion,
)


class XTRolledReason(XTDataPipeline):
    def __init__(self, local_test: bool) -> None:
        super().__init__(
            {
                "api_index": "prod_ihta_load_rolled",
                "columns": [
                    {"name": "Comment", "json_path": "", "replace": "rolled_comment"},
                    {
                        "name": "Created_at",
                        "json_path": "",
                        "replace": "created_ms",
                        "conversion": XTDataPipelineConversion.TimeinMs,
                    },
                    {"name": "Created_At_Ms", "json_path": "", "replace": "created_ms"},
                    {"name": "Created_by", "json_path": "", "replace": "created_by"},
                    {"name": "load_Id", "json_path": "load_id"},
                    {
                        "name": "Order_number",
                        "json_path": "order_number",
                        "conversion": XTDataPipelineConversion.Integer,
                    },
                    {"name": "Rolled_code", "json_path": "", "replace": "rolled_code"},
                    {
                        "name": "Rolled_code_description",
                        "json_path": "",
                        "replace": "rolled_code_description",
                    },
                    {
                        "name": "TMW_Number",
                        "json_path": "tmw_number",
                        "conversion": XTDataPipelineConversion.Integer,
                    },
                ],
                "db_table_name": "XNS_reporting.dbo.XT_Rolled_Reason",
            },
            local_test,
        )

    def process(self, db_connection=None) -> bool:
        try:
            cursor = None
            max_created_at_ms_timestamp = None

            if db_connection is not None:
                cursor = db_connection.cursor()

                sql = f"SELECT MAX(Created_At_Ms) FROM {self.db_table_name}"
                if self.local_test:
                    logger.info(f"SQL: {sql}")
                cursor.execute(sql)

                for row in cursor.fetchall():
                    if row[0] is not None:
                        max_created_at_ms_timestamp = int(row[0])

                if self.local_test:
                    logger.info(f"Max: {max_created_at_ms_timestamp}")

            for api_row in self.search(
                "load_roll_reasons.created_ms",
                max_created_at_ms_timestamp,
                "load_roll_reasons",
            ):
                self.insert_row(cursor, api_row)

            if db_connection is not None:
                db_connection.commit()

        except Exception as exception:
            self.handleException(exception)

        return self.successful
