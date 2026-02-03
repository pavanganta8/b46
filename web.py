from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# 1. Define the logic you want to execute
def print_hello():
    print("Hello World!")
    return "Hello World!"

# 2. Define the DAG
with DAG(
    dag_id='hello_world_dag',
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:

    # 3. Create the task
    hello_task = PythonOperator(
        task_id='print_hello_task',
        python_callable=print_hello
    )