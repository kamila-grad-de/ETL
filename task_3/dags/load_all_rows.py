from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

DATA_PATH = "/opt/airflow/output/iot_temp_filtered.csv"


def load_full_data():
    df = pd.read_csv(DATA_PATH, parse_dates=["date"])

    print("Load all historical data")
    print(df.head(5).to_string(index=False))
    print(df.tail(5).to_string(index=False))


with DAG(
    dag_id="load_full_data",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["load", "full"],
) as dag:
    load = PythonOperator(
        task_id="load_full_data",
        python_callable=load_full_data,
    )

    load