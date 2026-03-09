from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pymongo import MongoClient
from psycopg2.extras import execute_batch

MONGO_URI = "mongodb://airflow:airflow@mongodb:27017/?authSource=admin"
POSTGRES_CONN_ID = "postgres_default"
TARGET_SCHEMA = "analytics_data"


def sync_mongo_to_postgres():
    mongo = MongoClient(MONGO_URI)
    mongo_db = mongo["airflow"]

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute(
        f"""
        CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};

        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.user_sessions (
            session_id TEXT PRIMARY KEY,
            user_id TEXT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            device TEXT
        );

        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.user_session_pages (
            session_id TEXT,
            page TEXT
        );

        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.user_session_actions (
            session_id TEXT,
            action TEXT
        );

        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.event_logs (
            event_id TEXT PRIMARY KEY,
            timestamp TIMESTAMP,
            event_type TEXT
        );

        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.event_log_details (
            event_id TEXT,
            page TEXT,
            product_id TEXT
        );

        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.support_tickets (
            ticket_id TEXT PRIMARY KEY,
            user_id TEXT,
            status TEXT,
            issue_type TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.support_ticket_messages (
            ticket_id TEXT,
            sender TEXT,
            message TEXT,
            timestamp TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.user_recommendations (
            user_id TEXT,
            last_updated TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.user_recommendation_products (
            user_id TEXT,
            product_id TEXT
        );

        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.moderation_queue (
            review_id TEXT PRIMARY KEY,
            user_id TEXT,
            product_id TEXT,
            review_text TEXT,
            rating INT,
            moderation_status TEXT,
            submitted_at TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.moderation_flags (
            review_id TEXT,
            flag TEXT
        );
        """
    )

    cur.execute(
        f"""
        TRUNCATE TABLE
            {TARGET_SCHEMA}.user_session_pages,
            {TARGET_SCHEMA}.user_session_actions,
            {TARGET_SCHEMA}.user_sessions,
            {TARGET_SCHEMA}.event_log_details,
            {TARGET_SCHEMA}.event_logs,
            {TARGET_SCHEMA}.support_ticket_messages,
            {TARGET_SCHEMA}.support_tickets,
            {TARGET_SCHEMA}.user_recommendation_products,
            {TARGET_SCHEMA}.user_recommendations,
            {TARGET_SCHEMA}.moderation_flags,
            {TARGET_SCHEMA}.moderation_queue;
        """
    )

    sessions_data = []
    session_pages_data = []
    session_actions_data = []

    for doc in mongo_db["UserSessions"].find():
        sessions_data.append(
            (
                doc["session_id"],
                doc["user_id"],
                doc["start_time"],
                doc["end_time"],
                doc["device"],
            )
        )

        for page in doc.get("pages_visited", []):
            session_pages_data.append((doc["session_id"], page))

        for action in doc.get("actions", []):
            session_actions_data.append((doc["session_id"], action))

    if sessions_data:
        execute_batch(
            cur,
            f"""
            INSERT INTO {TARGET_SCHEMA}.user_sessions
            (session_id, user_id, start_time, end_time, device)
            VALUES (%s, %s, %s, %s, %s)
            """,
            sessions_data,
        )

    if session_pages_data:
        execute_batch(
            cur,
            f"""
            INSERT INTO {TARGET_SCHEMA}.user_session_pages
            (session_id, page)
            VALUES (%s, %s)
            """,
            session_pages_data,
        )

    if session_actions_data:
        execute_batch(
            cur,
            f"""
            INSERT INTO {TARGET_SCHEMA}.user_session_actions
            (session_id, action)
            VALUES (%s, %s)
            """,
            session_actions_data,
        )

    event_logs_data = []
    event_log_details_data = []

    for doc in mongo_db["EventLogs"].find():
        event_logs_data.append(
            (
                doc["event_id"],
                doc["timestamp"],
                doc["event_type"],
            )
        )

        details = doc.get("details", {})
        event_log_details_data.append(
            (
                doc["event_id"],
                details.get("page"),
                details.get("product_id"),
            )
        )

    if event_logs_data:
        execute_batch(
            cur,
            f"""
            INSERT INTO {TARGET_SCHEMA}.event_logs
            (event_id, timestamp, event_type)
            VALUES (%s, %s, %s)
            """,
            event_logs_data,
        )

    if event_log_details_data:
        execute_batch(
            cur,
            f"""
            INSERT INTO {TARGET_SCHEMA}.event_log_details
            (event_id, page, product_id)
            VALUES (%s, %s, %s)
            """,
            event_log_details_data,
        )

    support_tickets_data = []
    support_ticket_messages_data = []

    for doc in mongo_db["SupportTickets"].find():
        support_tickets_data.append(
            (
                doc["ticket_id"],
                doc["user_id"],
                doc["status"],
                doc["issue_type"],
                doc["created_at"],
                doc["updated_at"],
            )
        )

        for message in doc.get("messages", []):
            support_ticket_messages_data.append(
                (
                    doc["ticket_id"],
                    message["sender"],
                    message["message"],
                    message["timestamp"],
                )
            )

    if support_tickets_data:
        execute_batch(
            cur,
            f"""
            INSERT INTO {TARGET_SCHEMA}.support_tickets
            (ticket_id, user_id, status, issue_type, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            support_tickets_data,
        )

    if support_ticket_messages_data:
        execute_batch(
            cur,
            f"""
            INSERT INTO {TARGET_SCHEMA}.support_ticket_messages
            (ticket_id, sender, message, timestamp)
            VALUES (%s, %s, %s, %s)
            """,
            support_ticket_messages_data,
        )

    user_recommendations_data = []
    recommendation_products_data = []

    for doc in mongo_db["UserRecommendations"].find():
        user_recommendations_data.append(
            (
                doc["user_id"],
                doc["last_updated"],
            )
        )

        for product_id in doc.get("recommended_products", []):
            recommendation_products_data.append((doc["user_id"], product_id))

    if user_recommendations_data:
        execute_batch(
            cur,
            f"""
            INSERT INTO {TARGET_SCHEMA}.user_recommendations
            (user_id, last_updated)
            VALUES (%s, %s)
            """,
            user_recommendations_data,
        )

    if recommendation_products_data:
        execute_batch(
            cur,
            f"""
            INSERT INTO {TARGET_SCHEMA}.user_recommendation_products
            (user_id, product_id)
            VALUES (%s, %s)
            """,
            recommendation_products_data,
        )

    moderation_queue_data = []
    moderation_flags_data = []

    for doc in mongo_db["ModerationQueue"].find():
        moderation_queue_data.append(
            (
                doc["review_id"],
                doc["user_id"],
                doc["product_id"],
                doc["review_text"],
                doc["rating"],
                doc["moderation_status"],
                doc["submitted_at"],
            )
        )

        for flag in doc.get("flags", []):
            moderation_flags_data.append((doc["review_id"], flag))

    if moderation_queue_data:
        execute_batch(
            cur,
            f"""
            INSERT INTO {TARGET_SCHEMA}.moderation_queue
            (review_id, user_id, product_id, review_text, rating, moderation_status, submitted_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            moderation_queue_data,
        )

    if moderation_flags_data:
        execute_batch(
            cur,
            f"""
            INSERT INTO {TARGET_SCHEMA}.moderation_flags
            (review_id, flag)
            VALUES (%s, %s)
            """,
            moderation_flags_data,
        )

    conn.commit()
    cur.close()
    conn.close()
    mongo.close()


with DAG(
    dag_id="mongo_to_postgres_flatten",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "mongo", "postgres", "flatten"],
) as dag:
    sync_task = PythonOperator(
        task_id="sync_mongo_to_postgres",
        python_callable=sync_mongo_to_postgres,
    )
