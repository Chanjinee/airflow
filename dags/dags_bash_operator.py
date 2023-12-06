from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_operator", # 에어플로우 처음 들어왔을 때, 화면에서 보이는 이름 -> 덱 이름 / 일반적으로 덱 파일명과 덱 ID는 일치시키는 것이 좋음
    schedule="0 0 * * *", # cron 스케쥴로 해당 덱이 언제 실행되는지 알려주는 코드 / 분, 시, 일, 월, 요일 을 나타내는 5개의 구간이 있음
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"), # 덱이 언제부터 돌것인지 결정해주는 부분 / UTC : 국제 표준시임
    catchup=False, # start_date와 비교하여서 누락된 기간은 코드가 돌 것인지 결정하는 부분 / True일 경우, 누락된 구간은 한번에 전체적으로 돌아감
    # dagrun_timeout=datetime.timedelta(minutes=60), # 타임아웃 값을 정함 -> 특정 시간 이상 덱이 돌면 에러처리 비슷하게 하는듯함
    # tags=["example", "example2"], # 에어플로우 첫 화면에서 보이는 파란색 박스 값 설정해주는 부분 / 태그 설정 -> 해당 태그를 클릭 시 지정 가능
    # params={"example_key": "example_value"}, # 덱 선언 밑에 테스크들에게 공통적으로 넘겨줄 파라미터가 있을 경우 지정
) as dag:
    # 태스크를 생성해주는 부분
    bash_t1 = BashOperator( # 해당 변수의 이름이 태스크의 이름이 됨
        task_id="bash_t1", # 태스크의 이름 / 표면적으로 보여지는 태스크의 이름이 지정됨 / 객체명과 태스크 ID는 동일하게 하는 것이 일반적으로 편함
        bash_command="echo whoami", # 우리가 어떤 쉘 스크립트를 수행할 것인지를 적어줌
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )

    # 마지막으로 태스크들의 수행 단계를 적어주면 완성
    bash_t1 >> bash_t2
    