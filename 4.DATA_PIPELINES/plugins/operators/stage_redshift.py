from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 s3_bucket = 'udacity-dend',
                 s3_bucket_key = 'log_date',
                 target_table='staging_events',
                 copy_json_format = 'auto', #'s3://udacity-dend/log_json_path.json'
                 aws_region = 'us-west-2',
                 access_key_id = '',
                 secret_access_key = '',
                 ds = None,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_bucket_key = s3_bucket_key
        self.target_table = target_table
        self.copy_json_format = copy_json_format
        self.aws_region = aws_region
        self.ds = ds

        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key

        self.redshift_hook = PostgresHook(self.conn_id)




    def execute(self, context):
        self.log.info(f'Copying .json files from S3 to redshift table:  {self.target_table}')

        if self.ds is not None:
            bucket_key = f'{self.s3_bucket_key}/{self.ds.year}/{self.ds.month}/{self.ds}-events.json'
        else:
            bucket_key = self.s3_bucket_key

        COPY_JSON_SQL = """\
        COPY {table_name}
        FROM 's3://{bucket}/{bucket_key}'
        ACCESS_KEY_ID '{access_key_id}'
        SECRET_ACCESS_KEY '{secret_access_key}'
        REGION '{aws_region}'
        FORMAT AS json '{json_format}';
        """.format(
            table_name = self.target_table,
            bucket = self.s3_bucket,
            bucket_key = bucket_key,
            access_key_id = self.access_key_id,
            secret_access_key = self.secret_access_key,
            aws_region = self.aws_region,
            json_format = self.copy_json_format,
        )

        # self.redshift_hook.run(f'DELETE FROM {self.target_table};')
        self.redshift_hook.run(COPY_JSON_SQL)






