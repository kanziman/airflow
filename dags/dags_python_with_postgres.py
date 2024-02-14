from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_python_with_postgres',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:

    
    def insrt_postgres(ip, port, db, user, passwd, **kwargs):
        import pymysql
        from contextlib import closing

        with closing(pymysql.connect(host=ip, db=db, user=user, passwd=passwd, port=int(port))) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'insrt 수행'
                # sql = 'insert into py_opr_drct_insrt values (%s,%s,%s,%s);'
                # cursor.execute(sql,(dag_id,task_id,run_id,msg))
                sql = "INSERT INTO stock.kor_market_price (기준일, 시장구분, 시가, 고가, 저가, 종가, 수정주가, 거래량, 거래대금, 시가총액, 외국인시가총액, 신용잔고, 예탁금) VALUES ('2024-02-09', 'KOSPI', 2620.26, 2629.51, 2610.21, 2620.32, null, 430008992, 12493400, 2132380030, 710438980, 9672010, 50840200)";
                cursor.execute(sql)
                conn.commit()

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_args=['localhost', '3306', 'stock', 'root', 'test1234']
    )
        
    insrt_postgres