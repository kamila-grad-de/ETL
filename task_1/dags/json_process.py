from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests


def fetch_data():
    response = requests.get("https://raw.githubusercontent.com/LearnWebCode/json-example/master/pets-data.json")
    response.raise_for_status()
    return response.json()


def normalize(raw):
    result = []
    for item in raw.get("pets", []):
        foods = item.get("favFoods") or [None]
        for food in foods:
            result.append({
                "name": item.get("name"),
                "species": item.get("species"),
                "birth_year": item.get("birthYear"),
                "fav_food": food,
                "photo": item.get("photo"),
            })
    return result


def write_to_db(rows):
    if not rows:
        return
    hook = PostgresHook(postgres_conn_id='postgres_task_1')
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pets (
            name TEXT,
            species TEXT,
            birth_year INT,
            fav_food TEXT,
            photo TEXT);
            """)
        cur.execute("""TRUNCATE TABLE pets;""")
        cur.executemany(
            """
            INSERT INTO pets (name, species, birth_year, fav_food, photo)
            VALUES (%s, %s, %s, %s, %s);
            """,
            [
                (
                    r["name"],
                    r["species"],
                    r["birth_year"],
                    r["fav_food"],
                    r["photo"],
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
    dag_id="task_01_json_process",
    description="ETL pipeline for pets JSON",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "json", "postgres"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=task_extract,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=task_transform,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=task_load,
    )

    extract_task >> transform_task >> load_task
