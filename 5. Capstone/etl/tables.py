import logging

from pyspark.sql.functions import col
from pyspark.sql.functions import year, month, dayofmonth, weekofyear, date_format
from pyspark.sql.functions import sum as sp_sum

import etl
from etl import spark
from etl.utils import load_csv_data_with_spark

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


# fact table
def create_immigration_table(df_spark, output_dir):
    """ Creates immigration fact table and dumps it into csv file in ``output_dir``.
    Files are partitioned by ``year`` and ``month``.

    Parameters:
        df_spark : Spark dataframe with data required to create table
        output_dir (str) : directory where csv files will be written
    """
    df_spark=df_spark.drop_duplicates(['cicid'])

    # write immigration table to csv files partitioned by year and month
    df_spark.write.partitionBy(['i94yr', 'i94mon']).mode('overwrite').format('csv').option('header', 'true').save(output_dir)
    logger.info(f'Done writting immigration table in {output_dir}')

# dimension tables
def create_calendar_table(df_spark, output_dir):
    """ Creates calendar table and dumps it into csv file in ``output_dir``.
    Files are partitioned by ``year`` and ``month``.

    Parameters:
        df_spark : Spark dataframe with data required to create table
        output_dir (str) : directory where csv files will be written
    """

    # extract columns to create calendar table
    calendar_table = df_spark.select(
        col('arrdate').alias('date'),
        year('arrdate').alias('year'),
        month('arrdate').alias('month'),
        dayofmonth('arrdate').alias('day'),
        weekofyear('arrdate').alias('week'),
        date_format('arrdate','E').alias('weekday')
    ).drop_duplicates(['date'])

    # write calendar table to csv files partitioned by year and month
    calendar_table.write.partitionBy(['year', 'month']).mode('overwrite').format('csv').option('header', 'true').save(output_dir)
    logger.info(f'Done writting calendar table in {output_dir}')

def create_ports_table(spark, df_immigration, ports_file, airports_file, output_dir):
    """ Creates ports dimmension table and dumps it into csv files in ``output_dir``

    Parameters:
        spark : spark session
        df_immigration (Spark dataframe) : immigration dataset
        ports_file (str) : path to ports dataset
        airports_file (str) : path to airports dataset
        output_dir (str) : directory where csv file will be written
    """
    df_ports = spark.read.format('csv').option('header','true').load(ports_file)
    df_airports = spark.read.format('csv').option('header','true').load(airports_file)
    df_airports = df_airports.withColumnRenamed("name","airport_name")

    logger.debug('Number of rows in df_ports: %s', df_ports.count())
    logger.debug('Number of rows in df_airports: %s', df_airports.count())

    # join outer as we are interested on creating ports table (and not airports)
    df = df_ports.join(
        df_airports,
        (df_ports.code == df_airports.ident),
        'outer'
        )

    df = df.join(
        df_immigration,
        (df.code == df_immigration.i94port),
        'outer'
    )

    logger.debug('Number of rows after merging df_ports with df_airports: %s', df.count())
    ports_table = df.select(
        df.code,
        df.address,
        df.state,
        df.type,
        df.name,
        df.iso_country,
        df.iso_region,
        df.lat,
        df.long
        ).drop_duplicates(['code'])

    # write airports table to csv file
    ports_table.write.mode('overwrite').format('csv').option('header','true').save(output_dir)
    logger.info(f'Done writting ports table to {output_dir}')

def create_country_table(spark, countries_file, output_dir):
    """ Creates country fact table and dumps it into a csv file in ``output_dir``
    Parameters:
        spark : spark session
        countries_file (str) : path to file with countries dataset
        output_dir (str) : directory where csv files will be written

    Returns:
        Spark dataframe with immigration data
    """
    df_countries = spark.read.format('csv').option('header','true').load(countries_file)

    # write countries table to csv file
    df_countries.write.mode('overwrite').format('csv').option('header','true').save(output_dir)
    logger.info(f'Done writting country table to {output_dir}')

def create_us_state_table(spark, states_file, demographics_file, output_dir):
    """ Creates us_states fact table and dumps it into a csv file in ``output_dir``
    Parameters:
        spark : spark session
        countries_file (str) : path to file with us_states dataset
        output_dir (str) : directory where csv files will be written

    Returns:
        Spark dataframe with immigration data
    """

    df_states = spark.read.format('csv').option('header','true').load(states_file)

    df_city_demographics = spark.read.format('csv').option('header', 'true').load(demographics_file)
    df_states_demographics = df_city_demographics.groupby(df_city_demographics['State Code'], df_city_demographics['State']).agg(
        sp_sum('Male Population').alias('Male Population'),
        sp_sum('Female Population').alias('Female Population'),
        sp_sum('Total Population').alias('Total Population'),
        sp_sum('Number of Veterans').alias('Number of Veterans'),
        sp_sum('Foreign-born').alias('Foreign-born')
        )

    df = df_states.join(
        df_states_demographics,
        (df_states.code == df_states_demographics['State Code']),
        'inner')

    us_states_table = df.select(
        df.code,
        col('State').alias('name'),
        col('Male Population').alias('male_population'),
        col('Female Population').alias('female_population'),
        col('Total Population').alias('total_population'),
        col('Number of Veterans').alias('number_veterans'),
        col('Foreign-born').alias('number_foreign_born'),
        ).drop_duplicates(['code'])

    # write countries table to csv file
    us_states_table.write.mode('overwrite').format('csv').option('header','true').save(output_dir)
    logger.info(f'Created us_states table in {output_dir}')

# entrypoint
def main():
    """ Pipeline to create dimension model (DM) tables from stagging datasets """

    # load immigration stagging data
    imm_stag_df = load_csv_data_with_spark(spark=spark, input_dir=etl.IMM_STAG_DIR)

    # immigration
    create_immigration_table(df_spark=imm_stag_df, output_dir=etl.IMM_TAB_DIR)

    # calendar
    create_calendar_table(df_spark=imm_stag_df, output_dir=etl.CALENDAR_TAB_DIR)

    # ports
    create_ports_table(
        spark=spark,
        df_immigration=imm_stag_df,
        ports_file=etl.PORTS_STAG_FILE,
        airports_file=etl.AIRPORTS_STAG_FILE,
        output_dir=etl.PORTS_TAB_DIR
        )

    # country
    create_country_table(spark=spark, countries_file=etl.COUNTRIES_STAG_FILE, output_dir=etl.COUNTRY_TAB_DIR)

    # us state
    create_us_state_table(spark=spark, states_file=etl.STATES_STAG_FILE, demographics_file=etl.DEMOGRAPHICS_STAG_FILE, output_dir=etl.US_STATES_TAB_DIR)


if __name__ == '__main__':
    main()