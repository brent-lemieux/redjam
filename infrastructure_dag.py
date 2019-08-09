import datetime
import logging
import sys

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

sys.path.append('/usr/local/projects')
from redjam.infrastructure import create_infrastructure
from redjam import sql_initial


AWS_HOOK = AwsHook("aws_credentials_dwh")
CREDENTIALS = AWS_HOOK.get_credentials()
logging.info('%s, %s' % (CREDENTIALS.access_key, CREDENTIALS.secret_key))


def create_infrastructure_function():
    try:
        create_infrastructure(CREDENTIALS.access_key, CREDENTIALS.secret_key)
        logging.info('Successfully created infrastructure')
    except Exception as e:
        logging.exception(e)
        raise


dag = DAG(
    "redjam_create_and_load_2",
    schedule_interval='@once',
    start_date=datetime.datetime.now()
)

create_infrastructure_task = PythonOperator(
    task_id="create_infrastructure_task",
    python_callable=create_infrastructure_function,
    dag=dag
)

create_staging_events_task = PostgresOperator(
    task_id="create_staging_events_task",
    dag=dag,
    postgres_conn_id="aws_redjam_cluster",
    sql=sql_initial.staging_events_table_create
)

create_staging_songs_task = PostgresOperator(
    task_id="create_staging_songs_task",
    dag=dag,
    postgres_conn_id="aws_redjam_cluster",
    sql=sql_initial.staging_songs_table_create
)

create_songplay_task = PostgresOperator(
    task_id="create_songplay_task",
    dag=dag,
    postgres_conn_id="aws_redjam_cluster",
    sql=sql_initial.songplay_table_create
)

create_user_task = PostgresOperator(
    task_id="create_user_task",
    dag=dag,
    postgres_conn_id="aws_redjam_cluster",
    sql=sql_initial.user_table_create
)

create_song_task = PostgresOperator(
    task_id="create_song_task",
    dag=dag,
    postgres_conn_id="aws_redjam_cluster",
    sql=sql_initial.song_table_create
)

create_artist_task = PostgresOperator(
    task_id="create_artist_task",
    dag=dag,
    postgres_conn_id="aws_redjam_cluster",
    sql=sql_initial.artist_table_create
)

create_time_task = PostgresOperator(
    task_id="create_time_task",
    dag=dag,
    postgres_conn_id="aws_redjam_cluster",
    sql=sql_initial.time_table_create
)

create_infrastructure_task >> create_staging_events_task
create_infrastructure_task >> create_staging_songs_task
create_infrastructure_task >> create_songplay_task
create_infrastructure_task >> create_user_task
create_infrastructure_task >> create_artist_task
create_infrastructure_task >> create_song_task
create_infrastructure_task >> create_time_task
