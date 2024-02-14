from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum
from datetime import timedelta
from airflow.utils.state import State 
from airflow.decorators import task

with DAG(
    dag_id='dags_market_external_task_sensor',
    start_date=pendulum.datetime(2023,3,1, tz='Asia/Seoul'),
    schedule='0 23 * * *',
    catchup=False
) as dag:


    market_scrap_sensor = ExternalTaskSensor(
        task_id='market_scrap_sensor',
        external_dag_id='dags_python_import_func',
        external_task_id='market_value',
        allowed_states=[State.SUCCESS],
        execution_delta=timedelta(hours=1),
        poke_interval=10        #10초
    )
    
    
    @task(task_id="market_refine_task")
    def print_context(some_input):
        print(some_input)
        
    market_refine_task = print_context('market_refine_task 실행')
        
        
    external_task_sensor_c = ExternalTaskSensor(
    task_id='external_task_sensor_c',
    external_dag_id='dags_python_import_func',
    external_task_id='task_c',
    allowed_states=[State.SUCCESS],
    execution_delta=timedelta(hours=1),
    poke_interval=10        #10초
)
        
    external_task_sensor_c >> market_scrap_sensor >> market_refine_task