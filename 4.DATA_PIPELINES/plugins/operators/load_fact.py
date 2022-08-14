from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                conn_id = 'redshift',
                sql = '',
                target_table = '',
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql = sql
        self.target_table = target_table

        self.redshift_hook = PostgresHook(self.conn_id)


    def execute(self, context):
        self.log.info(f'Inserting data into {self.target_table}')

        SQL_INSERT = """INSERT INTO {target_table}
        {sql};""".format(target_table = self.target_table, sql = self.sql)

        self.redshift_hook.run(SQL_INSERT)