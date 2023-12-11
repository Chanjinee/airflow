from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_template",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_t1 = BashOperator(
        task_id='bash_t1',
        bash_command='echo "data_interval_end: {{ data_interval_end }}  "' # 실행시킬 커맨드 -? {{}} 안은  변수 처리 됨(템플릿 적용) / 에어플로우 공식 홈페이지 가면 어떤 변수를 제공하는지 나옴
    )

    bash_t2 = BashOperator(
        task_id='bash_t2',
        env={
            'START_DATE':'{{data_interval_start | ds }}', # 템플릿 적용하여 날짜 자동으로 진행되도록 함 '| ds'는 날짜 형식 처리를 위함(YYYY-MM-DD)
            'END_DATE':'{{data_interval_end | ds }}'
        },
        bash_command='echo $START_DATE && echo $END_DATE' # && -> 앞이 실행되면 뒤를 실행하겠다는 쉘 스크립트 명령어
    )

    bash_t1 >> bash_t2