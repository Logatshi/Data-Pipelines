from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator, 
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# Default arguments for DAG tasks
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2023, 1, 1),   
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,   
    'retry_delay': timedelta(minutes=5),   
    'catchup': False,  
    'schedule_interval': '@hourly'  
}

# DAG definition using modern decorator-based approach
@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
)
def final_project():
    
    # Starting point of the DAG
    start_operator = DummyOperator(task_id='Begin_execution')

    # Staging events data from S3 to Redshift
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",  
        aws_creds_id="aws_credentials",  
        s3_bucket="pipeproject",  
        s3_key="log_data",  
        redshift_table_name="staging_events",
        extra_params="FORMAT AS JSON 's3://pipeproject/log_json_path.json'"
    )

    # Staging songs data from S3 to Redshift
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",  
        aws_creds_id="aws_credentials",  
        s3_bucket="pipeproject",  
        s3_key="song_data",  
        redshift_table_name="staging_songs",
        extra_params="FORMAT AS JSON 'auto' TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL"
    )

    # Load fact table (songplays)
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",  
        aws_creds_id="aws_credentials",  
        table_name="songplays",
        sql_statement=SqlQueries.songplay_table_insert,
        append_data=False
    )

    # Load users dimension table
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",  
        aws_creds_id="aws_credentials",  
        table_name="users",
        sql_statement=SqlQueries.user_table_insert,
        append_data=False
    )

    # Load songs dimension table
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",  
        aws_creds_id="aws_credentials", 
        table_name="songs",
        sql_statement=SqlQueries.song_table_insert,
        append_data=False
    )

    # Load artists dimension table
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",  
        aws_creds_id="aws_credentials", 
        table_name="artists",
        sql_statement=SqlQueries.artist_table_insert,
        append_data=False
    )

    # Load time dimension table
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",  
        aws_creds_id="aws_credentials",  
        table_name="time",
        sql_statement=SqlQueries.time_table_insert,
        append_data=False
    )

    # Data quality checks after loading all data
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",  
        aws_creds_id="aws_credentials",  
        tables=['songs', 'artists', 'users', 'time', 'songplays'], 
        data_quality_checks=[
            {'check_sql': 'SELECT COUNT(*) FROM songs WHERE title IS NULL', 'expected_result': 0},
            {'check_sql': 'SELECT COUNT(*) FROM artists WHERE name IS NULL', 'expected_result': 0},
            {'check_sql': 'SELECT COUNT(*) FROM users WHERE first_name IS NULL', 'expected_result': 0},
            {'check_sql': 'SELECT COUNT(*) FROM time WHERE month IS NULL', 'expected_result': 0},
            {'check_sql': 'SELECT COUNT(*) FROM songplays WHERE userid IS NULL', 'expected_result': 0}
        ]
    )

    # End of the DAG
    end_operator = DummyOperator(task_id='Stop_execution')

    # Setting task dependencies
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
    [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator

# Instantiating the DAG
final_project_dag = final_project()