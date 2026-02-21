from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text

POSTGRES_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"


def build_user_activity_mart():
    engine = create_engine(POSTGRES_URI)
    with engine.begin() as conn:
        conn.execute(text("""
            create table if not exists etl.user_activity_mart as
            select
                user_id,
                count(*) as sessions_cnt,
                avg(duration_minutes) as avg_duration
            from etl.user_sessions
            group by user_id
        """))


def build_support_stats_mart():
    engine = create_engine(POSTGRES_URI)
    with engine.begin() as conn:
        conn.execute(text("""
            create table if not exists etl.support_stats_mart as
            select
                status,
                issue_type,
                count(*) as tickets_cnt,
                avg(resolution_hours) as avg_resolution
            from etl.support_tickets
            group by status, issue_type
        """))


with DAG(
    dag_id="build_datamarts",
    start_date=datetime(2026, 2, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="build_user_activity_mart",
        python_callable=build_user_activity_mart
    )

    t2 = PythonOperator(
        task_id="build_support_stats_mart",
        python_callable=build_support_stats_mart
    )

    t1 >> t2