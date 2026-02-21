from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, text

MONGO_URI = "mongodb://mongo:27017"
POSTGRES_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

DB_NAME = "etl_project"
SCHEMA = "etl"


def _iso(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    try:
        ts = pd.to_datetime(x)
        if pd.isna(ts):
            return None
        return ts.to_pydatetime().isoformat(sep=" ")
    except Exception:
        return str(x)


def _from_iso(x):
    if x is None:
        return None
    return datetime.fromisoformat(str(x))


# -------------------- SESSIONS --------------------

def extract_sessions():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db["UserSessions"]

    sessions = list(collection.find().limit(100))

    for s in sessions:
        if "_id" in s:
            s["_id"] = str(s["_id"])

    return sessions


def transform_sessions(ti):
    raw = ti.xcom_pull(task_ids="extract_sessions") or []
    df = pd.DataFrame(raw)

    if df.empty:
        return {"main": [], "pages": []}

    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")
    df["end_time"] = pd.to_datetime(df["end_time"], errors="coerce")

    df["duration_minutes"] = (df["end_time"] - df["start_time"]).dt.total_seconds() / 60

    df_main = df[[
        "session_id", "user_id",
        "start_time", "end_time",
        "duration_minutes", "device"
    ]].copy()

    df_main["start_time"] = df_main["start_time"].apply(_iso)
    df_main["end_time"] = df_main["end_time"].apply(_iso)

    pages = df[["session_id", "pages_visited"]].explode("pages_visited")
    pages = pages.rename(columns={"pages_visited": "page"})
    pages["page"] = pages["page"].astype(str)

    return {
        "main": df_main.to_dict("records"),
        "pages": pages.to_dict("records"),
    }


def load_sessions(ti):
    data = ti.xcom_pull(task_ids="transform_sessions") or {"main": [], "pages": []}

    engine = create_engine(POSTGRES_URI)
    with engine.begin() as conn:
        for r in data.get("main", []):
            payload = dict(r)
            payload["start_time"] = _from_iso(payload.get("start_time"))
            payload["end_time"] = _from_iso(payload.get("end_time"))

            conn.execute(text(f"""
                INSERT INTO {SCHEMA}.user_sessions
                    (session_id, user_id, start_time, end_time, duration_minutes, device)
                VALUES
                    (:session_id, :user_id, :start_time, :end_time, :duration_minutes, :device)
                ON CONFLICT (session_id) DO UPDATE SET
                    user_id = EXCLUDED.user_id,
                    start_time = EXCLUDED.start_time,
                    end_time = EXCLUDED.end_time,
                    duration_minutes = EXCLUDED.duration_minutes,
                    device = EXCLUDED.device
            """), payload)

        for r in data.get("pages", []):
            conn.execute(text(f"""
                INSERT INTO {SCHEMA}.session_pages (session_id, page)
                VALUES (:session_id, :page)
                ON CONFLICT DO NOTHING
            """), r)


def extract_tickets():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db["SupportTickets"]

    tickets = list(collection.find().limit(200))

    for t in tickets:
        if "_id" in t:
            t["_id"] = str(t["_id"])

    return tickets


def transform_tickets(ti):
    raw = ti.xcom_pull(task_ids="extract_tickets") or []
    df = pd.DataFrame(raw)

    if df.empty:
        return []

    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

    df["resolution_hours"] = (df["updated_at"] - df["created_at"]).dt.total_seconds() / 3600

    out = df[[
        "ticket_id", "user_id", "status",
        "issue_type", "created_at",
        "updated_at", "resolution_hours"
    ]].copy()

    out["created_at"] = out["created_at"].apply(_iso)
    out["updated_at"] = out["updated_at"].apply(_iso)

    return out.to_dict("records")


def load_tickets(ti):
    data = ti.xcom_pull(task_ids="transform_tickets") or []

    engine = create_engine(POSTGRES_URI)
    with engine.begin() as conn:
        for r in data:
            payload = dict(r)
            payload["created_at"] = _from_iso(payload.get("created_at"))
            payload["updated_at"] = _from_iso(payload.get("updated_at"))

            conn.execute(text(f"""
                INSERT INTO {SCHEMA}.support_tickets
                    (ticket_id, user_id, status, issue_type, created_at, updated_at, resolution_hours)
                VALUES
                    (:ticket_id, :user_id, :status, :issue_type, :created_at, :updated_at, :resolution_hours)
                ON CONFLICT (ticket_id) DO UPDATE SET
                    user_id = EXCLUDED.user_id,
                    status = EXCLUDED.status,
                    issue_type = EXCLUDED.issue_type,
                    created_at = EXCLUDED.created_at,
                    updated_at = EXCLUDED.updated_at,
                    resolution_hours = EXCLUDED.resolution_hours
            """), payload)


with DAG(
    dag_id="mongo_to_postgres_etl",
    start_date=datetime(2026, 2, 1),
    schedule=None,
    catchup=False,
) as dag:

    extract_sessions_task = PythonOperator(
        task_id="extract_sessions",
        python_callable=extract_sessions,
    )

    transform_sessions_task = PythonOperator(
        task_id="transform_sessions",
        python_callable=transform_sessions,
    )

    load_sessions_task = PythonOperator(
        task_id="load_sessions",
        python_callable=load_sessions,
    )

    extract_tickets_task = PythonOperator(
        task_id="extract_tickets",
        python_callable=extract_tickets,
    )

    transform_tickets_task = PythonOperator(
        task_id="transform_tickets",
        python_callable=transform_tickets,
    )

    load_tickets_task = PythonOperator(
        task_id="load_tickets",
        python_callable=load_tickets,
    )

    extract_sessions_task >> transform_sessions_task >> load_sessions_task
    extract_tickets_task >> transform_tickets_task >> load_tickets_task