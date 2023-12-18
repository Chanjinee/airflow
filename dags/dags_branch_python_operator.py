from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator

with DAG(
    dag_id='dags_branch_python_operator',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'), 
    schedule='0 1 * * *',
    catchup=False
) as dag:
    def select_random():
        import random

        item_lst = ['A','B','C']
        selected_item = random.choice(item_lst)
        if selected_item == 'A':
            return 'task_a' # 태스크 ID임
        elif selected_item in ['B','C']:
            return ['task_b','task_c'] # 여러개의 태스크를 실행하고 싶을 땐, 리스트 형태로 리턴
    
    python_branch_task = BranchPythonOperator( # BranchPythonOperator로 실행하는 함수의 return값은 후행 태스트의 ID임
        task_id='python_branch_task',
        python_callable=select_random
    )
    
    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )

    python_branch_task >> [task_a, task_b, task_c]