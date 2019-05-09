from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
import sql_statements


#create_trips_table = PostgresOperator(
#    task_id="create_trips_table",
#    dag=dag,
#    postgres_conn_id="redshift",
#    sql=sql_statements.CREATE_TRIPS_TABLE_SQL
#)

default_args = {
    'owner': 'dmm',
    'start_date': datetime(2019, 5, 7),
    'email': ['david.munoz4185@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past': False,
    'schedule_interval': '@daily'
}

dag = DAG(
    'udac_project_dag_create_tables',
    default_args=default_args,
    description='Drops and Creates Tables To Be Loaded in RedShift'#,
    #schedule_interval='0 * * * *'
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

drop_table_artists = PostgresOperator(
    task_id='Drop_artists',
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_ARTISTS,
    dag=dag
)

create_table_artists = PostgresOperator(
    task_id='Create_artists',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_ARTISTS,
    dag=dag
)

drop_table_songplays = PostgresOperator(
    task_id='Drop_songplays',
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_SONGPLAYS,
    dag=dag
)

create_table_songplays = PostgresOperator(
    task_id='Create_songplays',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_SONGPLAYS,
    dag=dag
)

drop_table_songs = PostgresOperator(
    task_id='Drop_songs',
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_SONGS,
    dag=dag
)

create_table_songs = PostgresOperator(
    task_id='Create_songs',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_SONGS,
    dag=dag
)

drop_table_staging_events = PostgresOperator(
    task_id='Drop_staging_events',
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_STAGING_EVENTS,
    dag=dag
)

create_table_staging_events = PostgresOperator(
    task_id='Create_staging_events',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_STAGING_EVENTS,
    dag=dag
)

drop_table_staging_songs = PostgresOperator(
    task_id='Drop_staging_songs',
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_STAGING_SONGS,
    dag=dag
)

create_table_staging_songs = PostgresOperator(
    task_id='Create_staging_songs',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_STAGING_SONGS,
    dag=dag
)

drop_table_time = PostgresOperator(
    task_id='Drop_time',
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_TIME,
    dag=dag
)

create_table_time = PostgresOperator(
    task_id='Create_time',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_TIME,
    dag=dag
)

drop_table_users = PostgresOperator(
    task_id='Drop_users',
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_USERS,
    dag=dag
)

create_table_users = PostgresOperator(
    task_id='Create_users',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_USERS,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> drop_table_artists
drop_table_artists >> create_table_artists
start_operator >> drop_table_songplays
drop_table_songplays >> create_table_songplays
start_operator >> drop_table_songs
drop_table_songs >> create_table_songs
start_operator >> drop_table_staging_events
drop_table_staging_events >> create_table_staging_events
start_operator >> drop_table_staging_songs
drop_table_staging_songs >> create_table_staging_songs
start_operator >> drop_table_time
drop_table_time >> create_table_time
start_operator >> drop_table_users
drop_table_users >> create_table_users
create_table_artists >> end_operator
create_table_songplays >> end_operator
create_table_songs >> end_operator
create_table_staging_events >> end_operator
create_table_staging_songs >> end_operator
create_table_time >> end_operator
create_table_users >> end_operator
