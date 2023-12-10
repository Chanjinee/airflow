from airflow import DAG
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    @task(task_id="python_task_1") # task 데커레이터를 사용하면 쉽게 파이썬 오퍼레이터를 사용 가능함 -> task는 어에플로우에서만 쓰이는 것은 아님
    def print_context(some_input):
        print(some_input)

    python_task_1 = print_context('task_decorator 실행')