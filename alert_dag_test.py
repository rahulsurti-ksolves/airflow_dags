from airflow.decorators import dag, task
from datetime import datetime, timedelta

# 1. Ye hamara Alert Function hai
# 'context' me Airflow automatically pass karta hai ki kaunsa task fail hua hai
def my_custom_alert(context):
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    error_msg = context.get('exception')
    
    # Real world me yahan Slack ya Email bhejne ka code hota hai
    print("=" * 50)
    print(f"🚨 ALERT NOTIFICATION 🚨")
    print(f"DAG: {dag_id} | TASK: {task_id}")
    print(f"Reason: {error_msg}")
    print("Sending Slack Message... (Simulated)")
    print("=" * 50)

# 2. Default args me hum apna alert function attach kar rahe hain
default_args = {
    "retries": 0, # Quick test ke liye retries 0 kar diye hain taaki turant fail ho
    "on_failure_callback": my_custom_alert, # Jaise hi fail hoga, ye trigger hoga
}

@dag(
    dag_id="alert_testing_dag",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["testing", "alerts"],
)
def alert_testing_dag():

    @task
    def failing_task_with_alert():
        print("Task start ho raha hai...")
        raise ValueError("Database connection lost!") # Jaan-boojh kar crash

    failing_task_with_alert()

alert_testing_dag()
