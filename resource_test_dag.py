from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from kubernetes.client import models as k8s

def allocate_memory():
    print("Starting memory allocation test...")
    # 500 MB ki dummy memory list create kar rahe hain
    dummy_data = bytearray(500 * 1024 * 1024)
    print("Successfully allocated ~500 MB RAM!")

# Task-level Kubernetes Resource Limits Define Karein
k8s_resource_config = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    resources=k8s.V1ResourceRequirements(
                        requests={"cpu": "250m", "memory": "256Mi"},  # Minimum required
                        limits={"cpu": "500m", "memory": "1Gi"}       # Maximum allowed
                    )
                )
            ]
        )
    )
}

with DAG(
    dag_id="k8s_resource_limit_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    test_task = PythonOperator(
        task_id="memory_heavy_task",
        python_callable=allocate_memory,
        executor_config=k8s_resource_config
    )
