from airflow.decorators import dag, task
from datetime import datetime, timedelta

# Default args me hum retries aur delay set kar rahe hain
default_args = {
    "retries": 2, # Task fail hone par 2 baar aur try karega
    "retry_delay": timedelta(seconds=30), # Har try ke beech sirf 30 second ka gap (quick test ke liye)
}

@dag(
    dag_id="test_retries_and_failures",
    default_args=default_args,
    schedule=None, # Manually trigger karenge
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["testing", "hands-on"],
)
def failing_dag_test():

    @task
    def successful_task():
        print("Main toh pass ho gaya!")
        return "Success"

    @task
    def failing_task():
        print("Ab main fail hone wala hu...")
        # Ye line jaan-boojh kar error produce karegi
        raise ValueError("Boom! Task achanak crash ho gaya.")

    # Dependencies set karna
    successful_task() >> failing_task()

# DAG ko call karna
failing_dag_test()
