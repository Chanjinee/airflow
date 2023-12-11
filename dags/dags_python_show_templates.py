from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task

with DAG(
    dag_id="dags_python_show_templates",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 3, 10, tz="Asia/Seoul"), # 해당 강의 찍는 당시 날짜 3/29
    catchup=True # 3/10일부터 3/29까지 모든 일정 코드 돌림
) as dag:
    
    @task(task_id='python_task')
    def show_templates(**kwargs):
        from pprint import pprint 
        pprint(kwargs)

    show_templates()