from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from datetime import datetime

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"]
)
def hello_world_dag():
    BashOperator(
        task_id="print_hello",
        bash_command="echo 'hello world'"
    )

dag = hello_world_dag()
