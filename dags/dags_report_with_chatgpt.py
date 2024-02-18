from airflow import DAG
import pendulum
from datetime import timedelta
from operators.report_by_chatgpt_operator import ReportByChatgptOperator

with DAG(
    dag_id='dags_report_with_chatgpt',
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule='0 13 * * *',
) as dag:
    write_report_by_chatgpt = ReportByChatgptOperator(
        task_id='write_report_by_chatgpt',
        post_cnt_per_market=1,
        model='gpt-3.5-turbo'
    )