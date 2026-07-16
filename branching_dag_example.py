from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import random

@dag(
    dag_id="branching_and_trigger_rules_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial", "branching"],
)
def branching_dag_example():

    # 1. Extract Task (BashOperator)
    extract = BashOperator(
        task_id="extract_data",
        bash_command="echo 'Extracting data from API...' && sleep 2",
    )

    # 2. Branching Task
    # Ye task decide karega ki data accha hai ya bura, aur agla task return karega
    @task.branch(task_id="check_data_quality")
    def check_data():
        is_data_valid = random.choice([True, False]) # Simulate checking logic
        
        if is_data_valid:
            print("Data is clean! Routing to success path.")
            return "process_valid_data" # Yeh us task ka ID hai jise run karna hai
        else:
            print("Data has errors! Routing to failure path.")
            return "send_error_alert"

    # 3. Path A: Valid Data Task
    @task(task_id="process_valid_data")
    def process_valid():
        print("Transforming and saving valid data...")
        return {"status": "success"}

    # 4. Path B: Invalid Data Task
    @task(task_id="send_error_alert")
    def send_alert():
        print("Sending Slack alert for bad data...")
        return {"status": "alert_sent"}

    # 5. Final Task (Trigger Rule ke saath)
    # trigger_rule zaroori hai kyuki Path A ya Path B me se ek hamesha skip hoga
    @task(
        task_id="final_report", 
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )
    def generate_report():
        print("DAG complete! Generating final summary report.")

    # ---------------------------------------------------------
    # ⚙️ WIRING: Tasks ko ek doosre se jodna (Dependencies)
    # ---------------------------------------------------------
    
    branch_task = check_data()
    path_a = process_valid()
    path_b = send_alert()
    final = generate_report()

    # Flow set karna:
    extract >> branch_task              # Pehle extract karo, fir check karo
    branch_task >> [path_a, path_b]     # Branching se do raste nikalte hain
    [path_a, path_b] >> final           # Dono raste aakar final report par milte hain

# DAG ko initialize karna
branching_dag_example()
