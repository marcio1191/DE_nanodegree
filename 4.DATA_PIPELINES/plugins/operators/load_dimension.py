from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id = 'redshift',
                 sql = '',
                 target_table = '',
                 append = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql = sql
        self.target_table = target_table

        self.append = append

        self.redshift_hook = PostgresHook(self.conn_id)


    def execute(self, context):
        self.log.info(f'Inserting data into {self.target_table}')

        SQL_INSERT = """INSERT INTO {target_table}
        {sql_insert}; """.format(
            target_table = self.target_table,
            sql_insert = self.sql
        )

        SQL_DELETE = """DELETE FROM {target_table};""".format(target_table = self.target_table)

        if self.append:
            self.redshift_hook.run(SQL_DELETE)
        self.redshift_hook.run(SQL_INSERT)