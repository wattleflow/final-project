from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
# from helpers import SqlQueries

class DataTransformOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(
        self,
        conn_id,
        table_name,
        sql_transform,
        *args,
        **kwargs
    ):
        super(DataTransformOperator, self).__init__(*args, **kwargs)
        self.conn_id  = conn_id
        self.table    = table_name
        self.sql      = sql_transform
        self.clsname  = type(self).__name__

    def execute(self, context):
        self.log.info(f"START - {self.clsname} - execute - {self.table}")
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        hook.run(self.sql)
        self.log.info(f"FINNISH   - {self.clsname} - execute: DONE")