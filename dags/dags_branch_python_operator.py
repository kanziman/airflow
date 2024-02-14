from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from common.kor_market_price import price_main
from common.kor_market_price_support import price_support_main
from common.kor_market_credit import credit_main
from common.kor_market_value import value_main
from airflow.utils.edgemodifier import Label

# == dags_python_with_branch_decorator
with DAG(
    dag_id='dags_branch_python_operator',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'), 
    schedule='0 22 * * *',
    catchup=False
) as dag:
    market_price = PythonOperator(
        task_id='market_price',
        python_callable=price_main
    )
        
    market_price_support = PythonOperator(
        task_id='market_price_support',
        python_callable=price_support_main
    )
    
    market_credit = PythonOperator(
        task_id='market_credit',
        python_callable=credit_main
    )
    
    market_value = PythonOperator(
        task_id='market_value',
        python_callable=value_main
    )
    
    market_price >> Label('kospi/kosdaq price scraped') >> [market_price_support, market_credit] >> Label('kospi/kosdaq value/credit scraped') >> market_value