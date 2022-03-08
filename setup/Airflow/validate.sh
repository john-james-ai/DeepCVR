# initialize the database tables
airflow db init

# print the list of active DAGs
airflow dags list

# prints the list of tasks in the "extract_dag" DAG
airflow tasks list extract_dag

# prints the hierarchy of tasks in the "extract_dag" DAG
airflow tasks list extract_dag --tree

# Run a script
python /home/john/projects/DeepCVR/deepcvr/airflow/dags/extract_dag.py