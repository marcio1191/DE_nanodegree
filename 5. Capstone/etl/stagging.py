""" This module contains logic to parse and validate raw data into the stagging area"""

import pathlib
import logging
import re
from datetime import datetime

import pandas as pd
from pyspark.sql.functions import udf, col
from pyspark.sql.types import \
                            StringType as Str, \
                            IntegerType as Int, \
                            StructType as R, \
                            StructField as Fld, \
                            DoubleType as Dbl

import etl
from etl import spark
from etl.utils import CleanPandasDf

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

# unit
def process_immigration_data_file(spark, input_file, output_dir):
    """ Processes a single immigration data file using spark and writes .csv files to ``output_dir`` partitioned by ``year`` and ``month``.

    Parameters:
        spark : Spark session
        input_file (str) : path to input file
        output_dir (str) : path to output directory where .csv files will be written
    """

    df_spark = spark.read.format('com.github.saurfang.sas.spark').load(input_file)

    schema = {
        "cicid": Int(),
        "i94yr": Int(),
        "i94mon": Int(),
        "i94cit": Int (),
        "i94res": Int (),
        "i94port": Str (),
        "arrdate": Int (),
        "i94mode": Int (),
        "i94addr": Str (),
        "depdate": Int(),
        "i94bir": Int (),
        "i94visa": Int (),
        "count": Int (),
        "dtadfile": Int (),
        "visapost": Str (),
        "occup": Str (),
        "entdepa": Str (),
        "entdepu": Str (),
        "matflag": Str (),
        "biryear": Int (),
        "dtaddto": Int (),
        "gender": Str (),
        "insnum": Int (),
        "airline": Str (),
        "admnum": Int (),
        "fltno": Int (),
        "visatype": Str (),
    }

    # enforcing column type
    for col_name, col_type in schema.items():
        df_spark = df_spark.withColumn(col_name, df_spark[col_name].cast(col_type))

    # convert date-type columns to datetime
    get_datetime = udf(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d') if x is not None else x) #  # if x is not None else x)
    df_spark = df_spark\
        .withColumn('arrdate', get_datetime(col('arrdate')))\
        .withColumn('dtaddto', get_datetime(col('dtaddto')))\
        .withColumn('depdate', get_datetime(col('depdate')))\
        .withColumn('dtadfile', get_datetime(col('dtadfile')))

    # drop eventual duplicates
    df_spark.drop_duplicates(['cicid'])

    # write dataframe to csv files partitioned by year and month
    df_spark.write.partitionBy(['i94yr', 'i94mon']).mode('append').format('csv').option('header', 'true').save(output_dir)

# pipelines
def process_immigration_data(spark, input_dir, output_dir):
    """
    Processes immigration dataset (sas files) using spark and writes .csv files to ``output_dir``.

    Parameters:
        spark : spark session
        input_data (str) : path to ``.sas7bdat`` files
        output_dir (str) : path where tables will be written
    """

    # get all files
    files = pathlib.Path(input_dir).rglob('*.sas7bdat') # f'{input_dir}/immigration_data'
    files = [str(f) for f in files]

    for f in files:
        logger.info(f'Processing {f} ...')
        process_immigration_data_file(spark, f, output_dir)

    logger.info('Done processing all immigration SAS files')

def process_sas_label_descriptions_file(file_path: str, output_countries: str, output_ports: str, output_modes: str, output_states: str):
    """ Parses "I94_SAS_Labels_Descriptions.SAS" and returns 4 dataframes with ("code", "name") as columns
        -  i94cntyl (pd.DataFrame) : "countries" data
        -  i94prtl (pd.DataFrame) : "port" data
        -  i94model (pd.DataFrame) : "mode of transportation" data
        -  i94cntyl (pd.DataFrame) : "USA states" data

    Parameters:
        file_path (str) : path to "I94_SAS_Labels_Descriptions.SAS" file
        output_countries (str) : path where countries df will be written
        output_ports (str) : path where ports df will be written
        output_modes (str) : path where modes of transportation df will be written
        output_states (str) : path where countries df will be written
    """

    with open(file_path, 'r') as f:
        file_content = f.read()

    content_groups = file_content.split('*') # split  by groups based on '*'

    d_groups_str = {
        'i94cntyl': [], # countries
        'i94prtl': [], # ports
        'i94model': [], # modes of transportation
        'i94addrl': [] # USA states
    }

    # capture str data into groups
    for group_str in content_groups:
        for group_name, group_str_list in d_groups_str.items():
            if group_name in group_str:
                group_str_list.append(group_str)
                break

    # validation
    for group_name, group_str_list in d_groups_str.items():
        if len(group_str_list) != 1:
            raise Exception(f'More than one "{group_name}" block was found')

        d_groups_str[group_name] = group_str_list[0]

    # match regex and create dataframe
    df_groups = dict()
    for group_name, content_str in d_groups_str.items():
        data = []
        regex = r"^\s*[\"\']?((\w|\d)+)[\"\']?\s*=\s*(\"|\')?([^\'\"\n]+)(\"|\')?.*$" # match key-pair values
        matches = re.finditer(regex, content_str, re.M)
        for match in matches:
            data.append(
                (match.group(1).strip(), match.group(4).strip())
            )
        df_groups[group_name] = pd.DataFrame(data, columns=['code', 'name'])

    # dataframes
    df_countries = df_groups['i94cntyl']
    df_ports = df_groups['i94prtl']
    df_modes_transport = df_groups['i94model']
    df_states = df_groups['i94addrl']

    # handle duplicates and split columns
    df_countries.drop_duplicates(subset=['code'], keep='last', inplace=True)

    df_ports.drop_duplicates(subset=['code'], keep='last', inplace=True)
    df_ports[['address', 'state']] = df_ports['name'].str.rsplit(',', expand=True, n=1)

    df_modes_transport.drop_duplicates(subset=['code'], keep='last', inplace=True)

    df_states.drop_duplicates(subset=['code'], keep='last', inplace=True)

    # write to output_dir
    pathlib.Path(output_countries).parents[0].mkdir(parents=True, exist_ok=True)
    pathlib.Path(output_ports).parents[0].mkdir(parents=True, exist_ok=True)
    pathlib.Path(output_modes).parents[0].mkdir(parents=True, exist_ok=True)
    pathlib.Path(output_states).parents[0].mkdir(parents=True, exist_ok=True)

    df_countries.to_csv(output_countries, index=False)
    logger.info(f'Done writting countries df to {output_countries}')

    df_ports.to_csv(output_ports, index=False)
    logger.info(f'Done writting ports df to {output_ports}')

    df_modes_transport.to_csv(output_modes, index=False)
    logger.info(f'Done writting modes of transport df to {output_modes}')

    df_states.to_csv(output_states, index=False)
    logger.info(f'Done writting US states df to {output_states}')

def process_airport_data(file_path: str, output_file: str):
    """Processes airport data and writes data to output_file

    Parameters:
        file_path (str) : path to dataset
        output_file (str) : path to output file

    """
    df_airports = pd.read_csv(file_path)

    # Clean df_airports
    logger.debug('Dimensions before cleaning: ', df_airports.shape)
    CleanPandasDf.strip_white_text_from_all_cells(df_airports)
    CleanPandasDf.lower_case_all_cells(df_airports)
    CleanPandasDf.drop_all_cols_if_missing_data_is_more_than_perc(df_airports)
    CleanPandasDf.drop_duplicated_rows(df_airports)
    CleanPandasDf.drop_empty_cols(df_airports)
    CleanPandasDf.drop_empty_rows(df_airports)
    df_airports = CleanPandasDf.split_column_based_on_separator(df_airports, 'coordinates', ['lat', 'long'], separator=',', force_types=True, new_cols_types=(float, float))
    logger.debug('Dimensions after cleaning: ', df_airports.shape)

    # write df_airports
    pathlib.Path(output_file).parents[0].mkdir(parents=True, exist_ok=True)
    df_airports.to_csv(output_file, index=False)
    logger.info(f'Done writting airports df to {output_file}')

def process_demographics_data(spark, file_path: str, output_file: str):
    """Processes demographics data and writes data to output_file

    Parameters:
        file_path (str) : path to dataset
        output_file (str) : path to output file
    """
    schema = R([
        Fld("City", Str()),
        Fld("State", Str()),
        Fld("Median Age", Dbl()),
        Fld("Male Population", Int()),
        Fld("Female Population", Int()),
        Fld("Total Population", Int()),
        Fld("Number of Veterans", Int()),
        Fld("Foreign-born", Int()),
        Fld("Average Household Size", Dbl()),
        Fld("State Code", Str()),
        Fld("Race", Str()),
        Fld("Count", Int()),
    ])

    df_city_demographics = spark.read.format('csv').option('header','true').option('delimiter', ';').load(file_path, schema=schema)
    df_city_demographics.drop_duplicates()

        # drop eventual duplicates
    df_city_demographics.drop_duplicates(['State Code'])

    # write dataframe to csv files partitioned by year and month
    df_city_demographics.write.mode('overwrite').format('csv').option('header', 'true').save(output_file)
    logger.info(f'Done writting city demographics df to {output_file}')

# entrypoint
def main():
    """ Pipeline to create stagging datasets from raw datasets """

    # immigration
    process_immigration_data(spark=spark, input_dir=etl.IMM_RAW_DIR, output_dir=etl.IMM_STAG_DIR)

    # immigration labels
    process_sas_label_descriptions_file(
        file_path=etl.SAS_LABELS_RAW_FILE,
        output_countries=etl.COUNTRIES_STAG_FILE,
        output_ports=etl.PORTS_STAG_FILE,
        output_modes=etl.MODES_TRANSP_STAG_FILE,
        output_states=etl.STATES_STAG_FILE,
    )

    # airports
    process_airport_data(file_path=etl.AIRPORTS_RAW_FILE, output_file=etl.AIRPORTS_STAG_FILE)

    # demographics
    process_demographics_data(spark, file_path=etl.DEMOGRAPHICS_RAW_FILE, output_file=etl.DEMOGRAPHICS_STAG_FILE)


if __name__ == '__main__':
    main()
