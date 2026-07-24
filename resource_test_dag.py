from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from kubernetes.client import models as k8s

def allocate_excessive_memory():
    print("Starting massive memory allocation test...")
    # Attempting to allocate ~2 GB RAM (Limit is 1 GB)
    dummy_data = bytearray(2000 * 1024 * 1024)
    print("This line will never print!")

k8s_resource_config = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    resources=k8s.V1ResourceRequirements(
                        requests={"cpu": "250m", "memory": "256Mi"},
                        limits={"cpu": "500m", "memory": "1Gi"}  # Strict Limit: 1GB
                    )
                )
            ]
        )
    )
}

with DAG(
    dag_id="k8s_oom_test_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    test_task = PythonOperator(
        task_id="force_oom_task",
        python_callable=allocate_excessive_memory,
        executor_config=k8s_resource_config
    )
