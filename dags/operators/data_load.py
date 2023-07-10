from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
# from helpers import SqlQueries

class DataLoadOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(
        self,
        conn_id,
        table_name,
        sql_insert,
        *args,
        **kwargs
    ):
        super(DataLoadOperator, self).__init__(*args, **kwargs)
        self.conn_id  = conn_id
        self.table    = table_name
        self.sql      = sql_insert
        self.clsname  = type(self).__name__

    def insert(self, hook):
        self.log.info(f"START - {self.clsname} - insert - {self.table}")
        hook.run(self.sql)
        self.log.info(f"FINNISH - {self.clsname} - insert - DONE")

    def truncate(self, hook):
        self.log.info(f"START - {self.clsname} - truncate - {self.table}")
        hook.run(f"TRUNCATE TABLE {self.table};")
        self.log.info(f"FINNISH  - {self.clsname} - truncate - DONE")

    def execute(self, context):
        self.log.info(f"START - {self.clsname} - execute - {self.table}")
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.truncate(hook)
        self.insert(hook)
        self.log.info(f"FINNISH - {self.clsname} - execute - DONE")