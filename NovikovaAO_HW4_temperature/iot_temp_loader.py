from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Date, Float, String

CSV_PATH = "/opt/airflow/data/IOT-temp.csv"
POSTGRES_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

SCHEMA = "hw4"
TABLE_FULL = "iot_temp_full"
TABLE_INCR = "iot_temp_incr"


def _read_and_transform():
    df = pd.read_csv(CSV_PATH)

    df = df.rename(columns={"room_id/id": "room_id", "out/in": "out_in"})

    df["noted_date"] = pd.to_datetime(df["noted_date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["noted_date"])
    df["noted_date"] = df["noted_date"].dt.date

    df["out_in"] = df["out_in"].astype(str).str.strip()
    df = df[df["out_in"].str.lower() == "in"]

    df["temp"] = pd.to_numeric(df["temp"], errors="coerce")
    df = df.dropna(subset=["temp"])

    cols = ["id", "room_id", "noted_date", "temp", "out_in"]
    df = df[[c for c in cols if c in df.columns]]
    return df


def load_full():
    df = _read_and_transform()

    engine = create_engine(POSTGRES_URI)
    with engine.begin() as conn:
        conn.execute(text(f"create schema if not exists {SCHEMA}"))
        df.to_sql(
            name=TABLE_FULL,
            con=conn,
            schema=SCHEMA,
            if_exists="replace",
            index=False,
            dtype={
                "id": String(),
                "room_id": String(),
                "noted_date": Date(),
                "temp": Float(),
                "out_in": String(),
            },
        )


def load_incremental():
    df = _read_and_transform()

    max_dt = df["noted_date"].max()
    window_start = max_dt - timedelta(days=3)
    df_win = df[df["noted_date"] >= window_start]

    engine = create_engine(POSTGRES_URI)
    with engine.begin() as conn:
        conn.execute(text(f"create schema if not exists {SCHEMA}"))
        conn.execute(text(f"""
            create table if not exists {SCHEMA}.{TABLE_INCR} (
                id         text primary key,
                room_id    text,
                noted_date date,
                temp       double precision,
                out_in     text
            )
        """))

        cnt = conn.execute(text(f"select count(1) from {SCHEMA}.{TABLE_INCR}")).scalar()
        if cnt == 0:
            df.to_sql(
                name=TABLE_INCR,
                con=conn,
                schema=SCHEMA,
                if_exists="append",
                index=False,
                dtype={
                    "id": String(),
                    "room_id": String(),
                    "noted_date": Date(),
                    "temp": Float(),
                    "out_in": String(),
                },
            )
            return

        upsert_sql = text(f"""
            insert into {SCHEMA}.{TABLE_INCR} (id, room_id, noted_date, temp, out_in)
            values (:id, :room_id, :noted_date, :temp, :out_in)
            on conflict (id) do update set
                room_id    = excluded.room_id,
                noted_date = excluded.noted_date,
                temp       = excluded.temp,
                out_in     = excluded.out_in
        """)
        for r in df_win.to_dict(orient="records"):
            conn.execute(upsert_sql, r)


with DAG(
    dag_id="iot_temp_hw4_pipeline",
    start_date=datetime(2026, 2, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    task_full = PythonOperator(
        task_id="load_full",
        python_callable=load_full
    )

    task_incr = PythonOperator(
        task_id="load_incremental",
        python_callable=load_incremental
    )

    task_full >> task_incr
