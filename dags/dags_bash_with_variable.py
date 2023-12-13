# 전역변수 -> 메일을 보낼 때, 해당 이메일 등 개발자들끼리 변하면 안되는 상수 등을 저장해서 관리
from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    dag_id="dags_bash_with_variable",
    schedule="10 9 * * *",
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # 1안 -> 권고하지 않는 방법 / 스케쥴러가 parsing해서 덱의 상태를 확인할 때, 모듈을 import하는 코드가 계속 실행되어 성능 저하
    var_value = Variable.get("sample_key")

    bash_var_1 = BashOperator(
    task_id="bash_var_1",
    bash_command=f"echo variable:{var_value}"
    )

    # 2안 -> 권고하는 방법
    bash_var_2 = BashOperator(
    task_id="bash_var_2",
    bash_command="echo variable:{{var.value.sample_key}}"
    )
