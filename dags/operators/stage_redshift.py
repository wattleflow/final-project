from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(
        self,
        conn_id,
        local_run,
        region,
        s3bucket,
        access_key,
        secret_key,
        *args,
        **kwargs
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id    = conn_id
        self.local_run  = local_run
        self.region     = region
        self.s3bucket   = s3bucket
        self.access_key = access_key
        self.secret_key = secret_key
        self.clsname    = type(self).__name__

    def get_s3_files(self):
        self.log.info(f"START   - {self.clsname} - get_s3_files({self.s3bucket})")
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        response = s3.list_objects_v2(Bucket=self.s3bucket)

        if 'Contents' in response:
            files = [obj['Key'] for obj in response['Contents']]
            self.log.info(f"FINNISH - {self.clsname} - get_s3_files({files})")
            return files
        else:
            self.log.info(f"FINNISH - {self.clsname} - get_s3_files(ERROR-NONE)")
            return []

    def copy_to_redshift(self, hook, table_name, csv_file_name):
        self.log.info(f"START   - {self.clsname} - copy_to_redshift({table_name}, {csv_file_name})")
        sql_statemnt = f"""
        COPY staging_{table_name} 
            FROM 's3://self.s3bucket/{csv_file_path}'
            REGION '{self.region}'
            ACCESS_KEY_ID '{self.access_key}'
            SECRET_ACCESS_KEY '{self.secret_key}'
            WITH (FORMAT CSV, HEADER True);
        """  
        hook.run(sql_statemnt)
        self.log.info(f"FINNISH - {self.clsname} - copy_to_redshift()")

    def copy_from_staging(self, hook):
        self.log.info(f"START   - {self.clsname} - copy_from_staging()")

        data = [
            {'name':'staging_suburbs',   'path':'/home/airflow/data/NHoodNameCentroids.csv'},
            {'name':'staging_shootings', 'path':'/home/airflow/data/NYPD_Shooting_Incident_Data__Historic_.csv'},
            {'name':'staging_arrests',   'path':'/home/airflow/data/NYPD_Arrest_Data__Year_to_Date_.csv'},
            {'name':'staging_calls',     'path':'/home/airflow/data/NYPD_Calls_for_Service__Year_to_Date_.csv'},
        ]
        
        for table in data:
            sql = f"""
                COPY {table['name']} 
                    FROM '{table['path']}'
                    WITH (FORMAT CSV, HEADER True);
                """
            hook.run(sql)

        self.log.info(f"FINNISH - {self.clsname} - copy_from_staging()")

    def execute(self, context):
        self.log.info(f"START   - {self.clsname} - execute()")
        
        hook = PostgresHook(postgres_conn_id=self.conn_id)

        if self.local_run:
            self.copy_from_staging(hook)
        else:
            for csv_name in s3_files:
                self.copy_from_s3(hook, csv_name)

        self.log.info(f"FINNISH - {self.clsname} - execute: DONE")

#         COPY {self.table_name} 
#             FROM '{self.s3_path}' 
#             ACCESS_KEY_ID '{self.access_key}'
#             SECRET_ACCESS_KEY '{self.secret_key}'
#             REGION '{self.region}'
#             JSON '{self.json_path}'
#             TIMEFORMAT as 'epochmillisecs'
#             TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
