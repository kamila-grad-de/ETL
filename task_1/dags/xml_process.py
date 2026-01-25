from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
from datetime import datetime
import requests
import xml.etree.ElementTree as ET

register_adapter(dict, Json)


def fetch_data():
    r = requests.get("https://gist.githubusercontent.com/pamelafox/3000322/raw/6cc03bccf04ede0e16564926956675794efe5191/nutrition.xml")
    r.raise_for_status()
    return r.text


def normalize(xml_text):
    root = ET.fromstring(xml_text)
    rows = []
    for food in root.findall("food"):
        serving = food.find("serving")
        calories = food.find("calories")
        vitamins = food.find("vitamins")
        minerals = food.find("minerals")
        rows.append({
            "name": food.findtext("name"),
            "manufacturer": food.findtext("mfr"),
            "serving_value": float(serving.text) if serving is not None and serving.text else None,
            "serving_units": serving.attrib.get("units") if serving is not None else None,
            "calories_total": float(calories.attrib.get("total")) if calories is not None and calories.attrib.get("total") else None,
            "calories_fat": float(calories.attrib.get("fat")) if calories is not None and calories.attrib.get("fat") else None,
            "total_fat": float(food.findtext("total-fat")) if food.findtext("total-fat") else None,
            "saturated_fat": float(food.findtext("saturated-fat")) if food.findtext("saturated-fat") else None,
            "cholesterol": float(food.findtext("cholesterol")) if food.findtext("cholesterol") else None,
            "sodium": float(food.findtext("sodium")) if food.findtext("sodium") else None,
            "carbs": float(food.findtext("carb")) if food.findtext("carb") else None,
            "fiber": float(food.findtext("fiber")) if food.findtext("fiber") else None,
            "protein": float(food.findtext("protein")) if food.findtext("protein") else None,
            "vitamins": {c.tag: float(c.text) if c.text else None for c in vitamins} if vitamins is not None else None,
            "minerals": {c.tag: float(c.text) if c.text else None for c in minerals} if minerals is not None else None,
        })
    return rows


def write_to_db(rows):
    if not rows:
        return
    hook = PostgresHook(postgres_conn_id="postgres_task_1")
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS foods (
                name TEXT,
                manufacturer TEXT,
                serving_value NUMERIC,
                serving_units TEXT,
                calories_total NUMERIC,
                calories_fat NUMERIC,
                total_fat NUMERIC,
                saturated_fat NUMERIC,
                cholesterol NUMERIC,
                sodium NUMERIC,
                carbs NUMERIC,
                fiber NUMERIC,
                protein NUMERIC,
                vitamins JSONB,
                minerals JSONB);
            """)

        cur.execute("TRUNCATE TABLE foods;")

        cur.executemany(
            """
            INSERT INTO foods (
                name, manufacturer, serving_value, serving_units,
                calories_total, calories_fat, total_fat, saturated_fat,
                cholesterol, sodium, carbs, fiber, protein, vitamins, minerals
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            [
                (
                    r["name"],
                    r["manufacturer"],
                    r["serving_value"],
                    r["serving_units"],
                    r["calories_total"],
                    r["calories_fat"],
                    r["total_fat"],
                    r["saturated_fat"],
                    r["cholesterol"],
                    r["sodium"],
                    r["carbs"],
                    r["fiber"],
                    r["protein"],
                    r["vitamins"],
                    r["minerals"],
                )
                for r in rows
            ],
        )
        conn.commit()


def task_extract(ti):
    ti.xcom_push(key="raw", value=fetch_data())


def task_transform(ti):
    raw = ti.xcom_pull(task_ids="extract_task", key="raw")
    cleaned = normalize(raw)
    ti.xcom_push(key="rows", value=cleaned)


def task_load(ti):
    rows = ti.xcom_pull(task_ids="transform_task", key="rows")
    write_to_db(rows)


with DAG(
    dag_id="task_01_xml_process",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    extract_task = PythonOperator(task_id="extract_task", python_callable=task_extract)
    transform_task = PythonOperator(task_id="transform_task", python_callable=task_transform)
    load_task = PythonOperator(task_id="load_task", python_callable=task_load)

    extract_task >> transform_task >> load_task
