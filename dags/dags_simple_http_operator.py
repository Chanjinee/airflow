# Package Import
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
import pendulum

with DAG(
    dag_id='dags_simple_http_operator',
    start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule=None
) as dag:

    '''서울시 정보'''
    tb_cycle_station_info = SimpleHttpOperator(
        task_id='tb_cycle_station_info',
        http_conn_id='openapi.seoul.go.kr', # 만들었던 커넥션 정보 ID
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleStationInfo/1/10/', # api키가 노출되면 안되기 때문에 airflow variables에 올려 놓음 (자체 마스킹 처리 등으로 인해 보안 UP)
        method='GET',
        headers={'Content-Type': 'application/json',
                        'charset': 'utf-8',
                        'Accept': '*/*'
                        }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_cycle_station_info') # 이전에 SimpleHttpOperator를 통해 받았던 데이터를 xcom에 저장했는데, 이 코드에서 pull을 받아오는 과정임
        import json
        from pprint import pprint

        pprint(json.loads(rslt))
        
    tb_cycle_station_info >> python_2()
