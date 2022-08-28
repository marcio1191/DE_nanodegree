
from pyspark.sql import SparkSession

spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
    enableHiveSupport().getOrCreate()

# Some required paths

stagging_output_dir = './datasets/stagging'
table_output_dir = './datasets/tables'

# RAW
IMM_RAW_DIR = '../../data/18-83510-I94-Data-2016/'
SAS_LABELS_RAW_FILE = '../../data/I94_SAS_Labels_Descriptions.SAS'
AIRPORTS_RAW_FILE = './airport-codes_csv.csv'
DEMOGRAPHICS_RAW_FILE = './us-cities-demographics.csv'

# STAGGING
IMM_STAG_DIR = stagging_output_dir + '/immigration'
COUNTRIES_STAG_FILE = stagging_output_dir + '/df_countries.csv'
PORTS_STAG_FILE = stagging_output_dir + '/df_ports.csv'
MODES_TRANSP_STAG_FILE = stagging_output_dir + '/df_modes_transport.csv'
STATES_STAG_FILE = stagging_output_dir + '/df_states.csv'
AIRPORTS_STAG_FILE = stagging_output_dir + '/df_airports.csv'
DEMOGRAPHICS_STAG_FILE = stagging_output_dir + '/df_city_demographics.csv'

# TABLES
IMM_TAB_DIR = table_output_dir + '/immigration'
CALENDAR_TAB_DIR = table_output_dir + '/calendar'
PORTS_TAB_DIR = table_output_dir + '/ports'
COUNTRY_TAB_DIR = table_output_dir + '/country'
US_STATES_TAB_DIR = table_output_dir + '/us_states'