from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Default arguments for the DAG
default_args = {
    "owner": "alex",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
        dag_id="example_python_dag",
        default_args=default_args,
        description="A simple Python DAG",
        schedule_interval=timedelta(days=1),  # Run every day
        start_date=datetime(2023, 12, 1),  # Start date for the DAG
        catchup=False,  # Don't run for previous dates
        tags=["example"],
) as dag:
    # Define a Python function to be called by the PythonOperator
    def print_hello():
        print("Hello, Airflow!")


    # Define the tasks
    task_1 = PythonOperator(
        task_id="task_1",
        python_callable=print_hello,  # Call the function
    )


    def print_goodbye():
        print("Goodbye, Airflow!")


    task_2 = PythonOperator(
        task_id="task_2",
        python_callable=print_goodbye,
    )

    # Define task dependencies
    task_1 >> task_2  # task_1 must run before task_2
