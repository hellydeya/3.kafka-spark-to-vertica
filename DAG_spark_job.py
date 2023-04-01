from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
import os
import logging
from datetime import datetime 

logging = logging.getLogger(__name__)

os.environ['JAVA_HOME']='/usr/local/openjdk-11'
os.environ['SPARK_HOME'] ='/opt/spark'
os.environ['PYTHONPATH'] = '/usr/bin'
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
 
with DAG(
        'DAG_spark_job',
        tags=['DAG_spark'],
        start_date=datetime(2022, 10, 1),
        end_date=None,
        schedule_interval=None,
        is_paused_upon_creation=True,
        max_active_runs=1,
        catchup = False
        ) as dag:
    
    stg_load = BashOperator(
        task_id='stg_load',
        bash_command=f'python3 /project/job_spark.py -ks {Variable.get("kafka_server")}  -kp {Variable.get("kafka_port")}  -kt {Variable.get("kafka_topic")}  -kc {Variable.get("kafka_cert")}  -ku {Variable.get("kafka_username")}  -kpas {Variable.get("kafka_password")} -ps {Variable.get("postgres_server")} -pp {Variable.get("postgres_port")} -pd {Variable.get("postgres_db")} -ptc {Variable.get("postgres_tbl_c")} -ptt {Variable.get("postgres_tbl_t")} -pu {Variable.get("postgres_user")} -ppas {Variable.get("postgres_password")}'
        )

stg_load