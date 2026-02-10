from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

DATA_PATH = "/opt/airflow/output/iot_temp_filtered.csv"


def load_all_rows():
    df = pd.read_csv(DATA_PATH, parse_dates=["date"])

    print("Load all historical data")
    print(df.head(5).to_string(index=False))
    print(df.tail(5).to_string(index=False))

    df.to_csv('/opt/airflow/output/all_rows.csv', index=False)


with DAG(
    dag_id="load_all_rows",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["load", "full"],
) as dag:
    load = PythonOperator(
        task_id="load_all_rows",
        python_callable=load_all_rows,
    )

    load