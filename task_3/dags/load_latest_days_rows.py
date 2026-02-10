from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

DATA_PATH = "/opt/airflow/output/iot_temp_filtered.csv"
DAYS = 5


def load_latest_days_rows():
    df = pd.read_csv(DATA_PATH, parse_dates=["date"])

    end = pd.to_datetime('2018-12-08')
    beg = end - pd.Timedelta(days=DAYS)
    df = df[(df["date"] >= beg) & (df["date"] <= end)]

    print(f"Loaded latest {DAYS} back data")
    print(df.head(5).to_string(index=False))
    print(df.tail(5).to_string(index=False))

    df.to_csv('/opt/airflow/output/latest_days_2018-12-08.csv', index=False)


with DAG(
    dag_id="load_latest_days_rows",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["load", "incremental"],
) as dag:
    load_latest_days_rows = PythonOperator(
        task_id="load_latest_days_rows",
        python_callable=load_latest_days_rows,
    )

    load_latest_days_rows