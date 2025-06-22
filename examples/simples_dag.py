
# simple_test_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Função simples para teste
def print_hello():
    print("Hello, Airflow!")
    return "Hello, Airflow!"

def print_goodbye():
    print("Goodbye, Airflow!")
    return "Goodbye, Airflow!"

# Definir o DAG
with DAG(
    dag_id='simple_test_dag',  # Nome do DAG
    start_date=datetime(2025, 6, 21),  # Data de início
    schedule_interval=None,  # Não será executado periodicamente
    catchup=False,  # Não faz o "catchup" de execuções passadas
    tags=['debug'],  # Tags para organização
) as dag:

    # Tarefas no DAG
    task_1 = PythonOperator(
        task_id='print_hello_task',
        python_callable=print_hello,
    )

    task_2 = PythonOperator(
        task_id='print_goodbye_task',
        python_callable=print_goodbye,
    )

    # Definindo a ordem das tarefas
    task_1 >> task_2
