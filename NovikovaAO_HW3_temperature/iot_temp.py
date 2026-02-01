from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Date, Float

CSV_PATH = "/opt/airflow/data/IOT-temp.csv"
POSTGRES_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
SCHEMA_NAME = "hw3"


def _read_and_clean():
    df = pd.read_csv(CSV_PATH)

    df = df.rename(columns={"room_id/id": "room_id", "out/in": "out_in"})

    df["noted_date"] = pd.to_datetime(df["noted_date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["noted_date"])
    df["noted_date"] = df["noted_date"].dt.date

    df["out_in"] = df["out_in"].astype(str).str.strip()
    df = df[df["out_in"].str.lower() == "in"]

    df["temp"] = pd.to_numeric(df["temp"], errors="coerce")
    df = df.dropna(subset=["temp"])

    return df


def compute_percentiles():
    df = _read_and_clean()
    p5, p95 = df["temp"].quantile([0.05, 0.95])
    return {"p5": float(p5), "p95": float(p95)}


def build_top5(ti):
    vals = ti.xcom_pull(task_ids="compute_percentiles")
    p5 = vals["p5"]
    p95 = vals["p95"]

    df = _read_and_clean()
    df = df[(df["temp"] >= p5) & (df["temp"] <= p95)]

    daily = df.groupby("noted_date", as_index=False)["temp"].mean().rename(columns={"temp": "mean_temp"})

    coldest = daily.sort_values("mean_temp", ascending=True).head(5).copy()
    hottest = daily.sort_values("mean_temp", ascending=False).head(5).copy()

    coldest["noted_date"] = coldest["noted_date"].astype(str)
    hottest["noted_date"] = hottest["noted_date"].astype(str)

    return {
        "coldest": coldest.to_dict(orient="records"),
        "hottest": hottest.to_dict(orient="records"),
    }


def load_to_db(ti):
    data = ti.xcom_pull(task_ids="build_top5")

    cold = pd.DataFrame(data["coldest"])
    hot = pd.DataFrame(data["hottest"])

    cold["noted_date"] = pd.to_datetime(cold["noted_date"]).dt.date
    hot["noted_date"] = pd.to_datetime(hot["noted_date"]).dt.date
    cold["mean_temp"] = pd.to_numeric(cold["mean_temp"])
    hot["mean_temp"] = pd.to_numeric(hot["mean_temp"])

    engine = create_engine(POSTGRES_URI)
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"))

        cold.to_sql(
            name="top_coldest",
            con=conn,
            schema=SCHEMA_NAME,
            if_exists="replace",
            index=False,
            dtype={"noted_date": Date(), "mean_temp": Float()},
        )
        hot.to_sql(
            name="top_hottest",
            con=conn,
            schema=SCHEMA_NAME,
            if_exists="replace",
            index=False,
            dtype={"noted_date": Date(), "mean_temp": Float()},
        )


with DAG(
    dag_id="iot_temp_pipeline",
    start_date=datetime(2026, 2, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    task_pctl = PythonOperator(
        task_id="compute_percentiles",
        python_callable=compute_percentiles
    )

    task_top5 = PythonOperator(
        task_id="build_top5",
        python_callable=build_top5
    )

    task_load = PythonOperator(
        task_id="load_to_db",
        python_callable=load_to_db
    )