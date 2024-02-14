from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum
from datetime import timedelta
from airflow.utils.state import State 
from airflow.decorators import task
from common.kor_market_refine import refine

with DAG(
    dag_id='dags_python_market_sensor',
    start_date=pendulum.datetime(2023,3,1, tz='Asia/Seoul'),
    schedule='0 22 * * *',
    catchup=False
) as dag:


    market_scrap_sensor = ExternalTaskSensor(
        task_id='market_scrap_sensor',
        external_dag_id='dags_python_import_func',
        poke_interval=10        #10초
    )
    
    
    @task(task_id="market_refine_task")
    def print_context(some_input):
        print(some_input)
        refine()
        
    market_refine_task = print_context('market_refine_task 실행')
        
        
    market_scrap_sensor >> market_refine_task