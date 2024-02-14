from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp
from common.kor_market_price import price_main
from common.kor_market_price_support import price_support_main
from common.kor_market_credit import credit_main
with DAG(
    dag_id="dags_python_import_func",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    task_get_sftp = PythonOperator(
        task_id='task_get_sftp',
        python_callable=get_sftp
    )
    
    task_get_sftp2 = PythonOperator(
        task_id='task_get_sftp2',
        python_callable=get_sftp
    )
    
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
    
    market_price >> [market_price_support, market_credit]