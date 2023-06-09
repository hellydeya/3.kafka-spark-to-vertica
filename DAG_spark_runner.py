from airflow import DAG
import logging
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.sensors.sql import SqlSensor
from datetime import datetime 

logging = logging.getLogger(__name__)

connection_pg = PostgresHook('PG_SOURCE_CONNECTION').get_conn() 

with DAG(
        'DAG_spark_runner',
        tags=['DAG_spark_runner'],
        start_date=datetime(2022, 10, 1),
        end_date=datetime(2022, 10, 31),
        schedule_interval="@daily",
        max_active_runs=1,
        catchup = True
        ) as dag:
    
    dag_trigger_spark = TriggerDagRunOperator(
        task_id='dag_trigger_spark',
        trigger_dag_id='DAG_spark_job',
        wait_for_completion=True,
        execution_date='{{ ds }}',
        reset_dag_run=True)
    
    wait_data_transaction = SqlSensor(
        task_id='wait_data_transaction',
        conn_id='PG_SOURCE_CONNECTION',
        sql="""SELECT * FROM public.temp_transaction WHERE transaction_dt::date = '{{ds}}'""",
        poke_interval=10,
        timeout=60)
    
    wait_data_currencies = SqlSensor(
        task_id='wait_data_currencies',
        conn_id='PG_SOURCE_CONNECTION',
        sql="""SELECT * FROM public.temp_currencies WHERE date_update::date = '{{ds}}'""",
        poke_interval=10,
        timeout=60)
    
    dag_trigger_postgres = TriggerDagRunOperator(
        task_id='dag_trigger_postgres',
        trigger_dag_id='DAG_pg_to_dwh',
        wait_for_completion=True,
        execution_date='{{ ds }}',
        reset_dag_run=True)

dag_trigger_spark >> [wait_data_transaction, wait_data_currencies] >> dag_trigger_postgres