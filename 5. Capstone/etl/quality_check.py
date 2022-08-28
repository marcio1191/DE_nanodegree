import logging

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

import etl
from etl import spark
from etl.utils import load_csv_data_with_spark

def check_df_has_records(spark_df, table_name):
    """Raises ValueError if ``spark_df`` has no records

    Parameters:
        spark_df : spark dataframe
        table_name (str) : name of the corresponding table

    Raises:
        ValueError: if dataframe has no records
    """
    if spark_df.count() == 0:
        raise ValueError(f'Table {table_name} has no records')
    logger.debug(f'Table {table_name} has records.')

def check_col_has_not_repeated_records(spark_df, table_name, col):
    """Checks a given column has no repeated records

    Parameters:
        spark_df : spark dataframe
        table_name (str) : name of the corresponding table
        col (str) : name of the table to be verified
    Raises:
        ValueError: if repeated values are found
    """
    max_count = spark_df.groupby(col).count().agg({"count": "max"}).collect()[0][0]
    if max_count > 1:
        raise ValueError(f'Table {table_name} has repeated values in column {col}')
    logger.debug(f'Column {col} in table {table_name} does not have repeated records.')

def main():
    """ Pipeline to validate all data model tables pass all the required quality checks:
        - ``check_df_has_records``
        - ``check_col_has_not_repeated_records``
    """

    tables = {
        'immigration_table': load_csv_data_with_spark(spark, etl.IMM_TAB_DIR),
        'calendar_table': load_csv_data_with_spark(spark, etl.CALENDAR_TAB_DIR),
        'ports_table': load_csv_data_with_spark(spark, etl.PORTS_TAB_DIR),
        'country_table': load_csv_data_with_spark(spark, etl.COUNTRY_TAB_DIR),
        'us_states_table': load_csv_data_with_spark(spark, etl.US_STATES_TAB_DIR)
    }
    tables_pks = {
        'immigration_table': 'cicid',
        'calendar_table': 'date',
        'ports_table': 'code',
        'country_table': 'code',
        'us_states_table': 'code',
    }

    for table_name in tables.keys():
        df = tables[table_name]
        pk = tables_pks[table_name]
        check_df_has_records(df, table_name) # validate table is not empty
        check_col_has_not_repeated_records(df, table_name, pk) # validate all pk records are unique