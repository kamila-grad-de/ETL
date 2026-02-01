from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
from io import StringIO


CSV_URL = "https://raw.githubusercontent.com/sushant1827/Temperature-Forecasting-using-IoT-Data/refs/heads/main/IOT-temp.csv"


def filter_iot_temperature_csv(**context):
    response = requests.get(CSV_URL, timeout=30)
    response.raise_for_status()

    df = pd.read_csv(StringIO(response.text))

    top = (
        df[["noted_date", "temp"]]
            .groupby("noted_date", as_index=False)
            .mean()
            .sort_values("temp")
            .reset_index(drop=True)
    )
    top.head(5).to_csv("/opt/airflow/output/iot_temp_coldest_days.csv", index=False)
    top.tail(5).to_csv("/opt/airflow/output/iot_temp_hottest_days.csv", index=False)

    df = df[df["out/in"] == "In"].reset_index(drop=True)
    df["date"] = pd.to_datetime(df["noted_date"], format="%d-%m-%Y %H:%M").dt.date
    df = df[(df["temp"] > df["temp"].quantile(0.05)) & (df["temp"] < df["temp"].quantile(0.95))].reset_index(drop=True)
    df.to_csv("/opt/airflow/output/iot_temp_filtered.csv", index=False)


with DAG(
    dag_id="filter_iot_temperature_data",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["csv", "pandas", "etl"],
) as dag:

    filter_iot_temperature = PythonOperator(
        task_id="filter_iot_temperature",
        python_callable=filter_iot_temperature_csv,
    )

    filter_iot_temperature
