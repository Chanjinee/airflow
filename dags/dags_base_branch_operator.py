from airflow import DAG
import pendulum
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_base_branch_operator',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    class CustomBranchOperator(BaseBranchOperator): # 공식 가이드를 보면, 무조건 해당 클래스를 상속해야함
        def choose_branch(self, context): # 무조건 이 함수를 만들고 오버라이딩 하도록 가이드에 나옴 (이름도 바꾸면 안됨, 안에 들어가는 인자도 변경 X)
            import random
            print(context) # 파라미터들이 들어있음(data_interval_start, 등 시간, 등에 관련된 파라미터 -> 이후 꺼내서 사용하면 됨)
            
            item_lst = ['A', 'B', 'C']
            selected_item = random.choice(item_lst)
            if selected_item == 'A':
                return 'task_a'
            elif selected_item in ['B','C']:
                return ['task_b','task_c']

    
    custom_branch_operator = CustomBranchOperator(task_id='python_branch_task') #id는 개인이 지정하는 값

    
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

    custom_branch_operator >> [task_a, task_b, task_c]