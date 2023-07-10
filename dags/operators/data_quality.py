from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        conn_id = "",
        tables = [],
        *args, 
        **kwargs
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.tables  = tables
        self.clsname = type(self).__name__

    def check_records(self, table, records):
        self.log.info(f"START - {self.clsname} - check_tables({table}, {records[0]})")

        has_records = len(records) < 1 or len(records[0]) < 1
        num_records = records[0][0]

        if has_records:
            msg = f"Quality chek for {table} failed with missing or no records."
            self.log.error(msg)
            raise ValueError(msg)

        if num_records == 0:
            msg = f"Quality chek for {table} failed with {str(num_records)} records."
            self.log.error(msg)
            raise ValueError(msg)

        self.log.info(f"FINNISH   - {self.clsname} - check_tables ({num_records})")

    def execute(self, context):
        self.log.info(f"START - {self.clsname} - execute:[{self.tables}]")
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        for table in self.tables:
            sql = f"SELECT COUNT(*) FROM {table};"
            records = hook.get_records(sql)
            isok = self.check_records(table, records)
        self.log.info(f"FINNISH  - {self.clsname} - execute: DONE")
