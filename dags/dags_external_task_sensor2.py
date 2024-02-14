from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum
from datetime import timedelta
from airflow.utils.state import State 

with DAG(
    dag_id='dags_external_task_sensor2',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule='0 23 * * *',
    catchup=False
) as dag:
    external_task_sensor_a = ExternalTaskSensor(
        task_id='external_task_sensor_a',
        external_dag_id='dags_python_import_func',
        external_task_id='market_value',
        allowed_states=[State.SKIPPED],
        execution_delta=timedelta(hours=1),
        poke_interval=10        #10초
    )

    market_scrap_sensor = ExternalTaskSensor(
        task_id='market_scrap_sensor',
        external_dag_id='dags_python_import_func',
        external_task_id='market_value',
        allowed_states=[State.SUCCESS],
        execution_delta=timedelta(hours=1),
        poke_interval=10        #10초
    )

    external_task_sensor_a >> market_scrap_sensor