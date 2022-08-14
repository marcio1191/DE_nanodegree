from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id = 'redshift',
                 table_cols_nulls_dict = {},
                 table_expected_num_rows = {},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.redshift_hook = PostgresHook(self.conn_id)

        self.table_cols_nulls_dict = table_cols_nulls_dict
        self.table_expected_num_rows = table_expected_num_rows

    def execute(self, context):

        self.log.info('Checking for NULLs ...')
        COUNT_NULL_SQL = """SELECT COUNT(*) FROM {table} WHERE {col} IS NULL;"""

        for table_name, columns in self.table_cols_nulls_dict.items():
            for col_name in columns:
                sql = COUNT_NULL_SQL.format(
                    table = table_name,
                    col = col_name
                )
                self.log.info(f'Counting number of nulls in column: {col_name} of table: {table_name} ...')
                num_nulls = self.redshift_hook.get_records(sql)[0][0]
                self.log.info(f'num NULLs = {num_nulls}')
                if num_nulls > 0:
                    msg = f'There are some nulls in {col_name} of table {table_name}'
                    raise ValueError(msg)


        self.log.info('Verifying the number of records ...')
        COUNT_SQL = """SELECT COUNT(*) FROM {table};"""

        for table_name, expected_number_rows in self.table_expected_num_rows.items():
            self.log.info(f'Counting number of rows in table: {table_name} ...')
            sql = COUNT_SQL.format(table = table_name)
            num_rows = self.redshift_hook.get_records(sql)[0][0]
            self.log.info(f'Number of rows in table {table_name} = {num_rows}')
            if num_rows != expected_number_rows:
                raise ValueError(f'Number of rows in table {table_name} = {num_rows}. However, expected value was {expected_number_rows}')

