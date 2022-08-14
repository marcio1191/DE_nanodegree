from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *' # hourly # None
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    s3_bucket = 'udacity-dend',
    s3_bucket_key = 'log_data',
    target_table='staging_events',
    copy_json_format = 's3://udacity-dend/log_json_path.json',
    aws_region = 'us-west-2',
    access_key_id = AWS_KEY,
    secret_access_key = AWS_SECRET,
    provide_context = True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    s3_bucket = 'udacity-dend',
    s3_bucket_key = 'song_data',
    target_table='staging_songs',
    copy_json_format = 'auto',
    aws_region = 'us-west-2',
    access_key_id = AWS_KEY,
    secret_access_key = AWS_SECRET,
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    conn_id = 'redshift',
    sql = SqlQueries.songplay_table_insert,
    target_table = 'songplays',
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    conn_id = 'redshift',
    sql = SqlQueries.user_table_insert,
    target_table = 'users',
    append = False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id = 'redshift',
    sql = SqlQueries.song_table_insert,
    target_table = 'songs',
    append = False

)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id = 'redshift',
    sql = SqlQueries.artist_table_insert,
    target_table = 'artists',
    append = False

)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id = 'redshift',
    sql = SqlQueries.time_table_insert,
    target_table = 'time',
    append = False
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id = 'redshift',
    table_cols_nulls_dict = {
            'songplays': ['playid', 'userid', 'sessionid', 'start_time'],
            'songs': ['songid', 'artistid', 'title'],
            'users': ['userid',],
            'artists': ['artistid', 'name'],
            'time': ['start_time', ]
        },
    table_expected_num_rows = {
        'songplays': 6820,
        'songs': 14896,
        'users': 312,
        'artists': 10025,
        'time': 6820,
    }
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# task dependencies
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator

