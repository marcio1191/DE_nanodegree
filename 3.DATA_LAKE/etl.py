import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import \
                                StructType as R, \
                                StructField as Fld, \
                                DoubleType as Dbl, \
                                StringType as Str, \
                                IntegerType as Int, \
                                TimestampType as Ts

config = configparser.ConfigParser()
# config.read('dl.cfg')
config.read_file(open('dl.cfg'))

# os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ 
    Processes song data using spark and writes parquet files to ``output_data``
    
    Args:
        spark (SparkSession) : spark session
        input_data (str) : URL to S3 bucket with .json data
        output_data (str) : path where parquet files will be saved
    
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    song_data_schema = R([
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_name", Str()),
        Fld("duration", Dbl()),
        Fld("num_songs", Int()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("year", Int()),
    ])
    df = spark.read.json(song_data, schema=song_data_schema)

#     # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).drop_duplicates(['song_id'])
    
#     # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + '/songs_table', partitionBy=['artist_id', 'year'], mode='overwrite')

#     # extract columns to create artists table
    artist_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']).drop_duplicates(['artist_id'])

    #     # write artists table to parquet files
    artist_table.write.parquet(output_data + '/artist_table', mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """ 
    Processes log data using spark and writes parquet files to ``output_data``
    
    Args:
        spark (SparkSession) : spark session
        input_data (str) : URL to S3 bucket with .json data
        output_data (str) : path where parquet files will be saved
    
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    log_data_schema = R([
        Fld("artist", Str()),
        Fld("auth", Str()),
        Fld("firstName", Str()),
        Fld("gender", Str()),
        Fld("itemInSession", Int()),
        Fld("lastName", Str()),
        Fld("length", Dbl()),
        Fld("level", Str()),
        Fld("location", Str()),
        Fld("method", Str()),
        Fld("page", Str()),
        Fld("registration", Str()),
        Fld("sessionId", Int()),
        Fld("song", Str()),
        Fld("status", Str()),
        Fld("ts", Str()),
        Fld("userAgent", Str()),
        Fld("userId", Str()),    
    ])

    df = spark.read.json(log_data, schema=log_data_schema)
    
    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')

    # extract columns for users table    
    users_table = df.select(
        col('userId').alias('user_id'),
        col('firstName').alias('first_name'),
        col('lastName').alias('last_name'),
        'gender',
        'level'
    ).drop_duplicates(['user_id'])
    
    # write users table to parquet files
    users_table.write.parquet(output_data + '/users_table', mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int((int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('date', get_datetime(df.timestamp))
    
    # extract columns to create time table
    time_table = df.select(
        col('timestamp').alias('start_time'),
        hour('date').alias('hour'),
        dayofmonth('date').alias('day'),
        weekofyear('date').alias('week'),
        month('date').alias('month'),
        year('date').alias('year'),
        date_format('date','E').alias('weekday')
    ).drop_duplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + '/time_table', partitionBy=['year', 'month'], mode='overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + '/songs_table')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(
        song_df,
        (df.song == song_df.title) & (df.length == song_df.duration),
        'inner').select(
        df.timestamp,
        col("userId").alias('user_id'),
        df.level,
        song_df.song_id,
        song_df.artist_id,
        col("sessionId").alias("session_id"),
        df.location,
        col("useragent").alias("user_agent"),
        year('date').alias('year'),
        month('date').alias('month')).drop_duplicates(['timestamp'])

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + '/songplays_table', partitionBy=['year', 'month'], mode='overwrite')


def main():
    """ Processes .json files under S3 with Spark and writes parquet files. """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-tables-marcio"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
