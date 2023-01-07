- In "airflow" folder: curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.2.3/docker-compose.yaml'
- In "airflow" folder: mkdir ./dags ./plugins ./logs
- In "airflow" folder: echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
- In "airflow" folder: docker-compose up airflow-init
- In "airflow" folder: docker-compose up (run container)


