from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging
import os
import sys


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../opt/airflow/src')))

from from_mongo_to_postgres import from_mongo_to_postgres
from sql_report import generate_daily_report

logger = logging.getLogger('dag_logger')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

dag = DAG(
    dag_id="stocks_dag",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 5, 1),
    max_active_runs=1
)

def start_job():
    logging.info("Starting the pipeline.")

def fetch_data_from_mongo():
    try:
        logger.info("Fetching data from MongoDB")
        from_mongo_to_postgres()
        logger.info("The data has been inserted into Postgresql")

    except Exception as e:
        logger.error(f"An error occured while fetching data {e}")

def daily_report():
    generate_daily_report()
    logger.info("The daily report prepared and saved into CSV files")

def end_job():
    logger.info("All process completed.")


start_task = PythonOperator(
    task_id='start_job',
    python_callable=start_job,
    dag=dag
)

fetching_data_task = PythonOperator(
    task_id='fetch_data_job',
    python_callable=fetch_data_from_mongo,
    dag=dag
)

daily_report_task = PythonOperator(
    task_id='daily_report_job',
    python_callable= daily_report,
    dag = dag
)

end_task = PythonOperator(
    task_id= 'end_job',
    python_callable=end_job,
    dag=dag
)

start_task >> fetching_data_task >> daily_report_task >> end_task
