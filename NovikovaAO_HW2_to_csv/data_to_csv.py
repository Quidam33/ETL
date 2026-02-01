from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import csv
import xml.etree.ElementTree as ET

DATA_JSON = "/opt/airflow/data/pets-data.json"
OUTPUT_JSON = "/opt/airflow/output/pets-json-linear.csv"

DATA_XML = "/opt/airflow/data/nutrition.xml"
OUTPUT_XML = "/opt/airflow/output/nutrition-linear.csv"

def json_to_csv():
    try:
        with open(DATA_JSON, "r") as f:
            data = json.load(f)
        pets = data.get("pets", [])
        rows = []
        for pet in pets:
            row = pet.copy()
            fav = row.get("favFoods")
            if fav:
                row["favFoods"] = "|".join(fav)
            rows.append(row)
        if not rows:
            return
        keys = rows[0].keys()
        with open(OUTPUT_JSON, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(rows)
    except Exception as e:
        print(e)

def xml_to_csv():
    try:
        tree = ET.parse(DATA_XML)
        root = tree.getroot()
        rows = []
        for food in root.findall("food"):
            row = {}
            for child in food:
                if child.tag in ["vitamins", "minerals"]:
                    for sub in child:
                        row[f"{child.tag}_{sub.tag}"] = sub.text
                elif child.tag == "calories":
                    for k, v in child.attrib.items():
                        row[f"calories_{k}"] = v
                else:
                    row[child.tag] = child.text
            rows.append(row)
        if not rows:
            return
        keys = rows[0].keys()
        with open(OUTPUT_XML, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(rows)
    except Exception as e:
        print(e)

with DAG(
    dag_id="data_to_csv",
    start_date=datetime(2026, 2, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    task_json = PythonOperator(
        task_id="json_to_csv",
        python_callable=json_to_csv
    )

    task_xml = PythonOperator(
        task_id="xml_to_csv",
        python_callable=xml_to_csv
    )

    task_json >> task_xml
