from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
import os

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
API_TOKEN = os.getenv("AQICN_API_TOKEN")

CITIES = [
    "Chiang Mai",
    "Chiang Rai",
    "Lampang",
    "Lamphun",
    "Phayao",
    "Phrae",
    "Nan",
    "Mae Hong Son"
]

BASE_URL = "https://api.waqi.info/feed/{city}/?token={token}"

DATA_DIR = "/opt/airflow/data"
os.makedirs(DATA_DIR, exist_ok=True)

# ─────────────────────────────────────────────
# FUNCTIONS
# ─────────────────────────────────────────────
def fetch_city_data(city):
    """Fetch air quality data for one city."""
    if not API_TOKEN:
        raise ValueError("AQICN_API_TOKEN is not set")
    url = BASE_URL.format(city=city.replace(" ", "%20"), token=API_TOKEN)
    resp = requests.get(url)
    if resp.status_code == 200:
        data = resp.json()
        if data.get("status") == "ok":
            info = data["data"]
            return {
                "city": city,
                "aqi": info.get("aqi"),
                "dominentpol": info.get("dominentpol"),
                "time": info.get("time", {}).get("s"),
                "pm25": info.get("iaqi", {}).get("pm25", {}).get("v"),
                "pm10": info.get("iaqi", {}).get("pm10", {}).get("v"),
                "o3": info.get("iaqi", {}).get("o3", {}).get("v"),
                "no2": info.get("iaqi", {}).get("no2", {}).get("v"),
                "so2": info.get("iaqi", {}).get("so2", {}).get("v"),
                "co": info.get("iaqi", {}).get("co", {}).get("v"),
            }
    else:
        print(f"❌ Failed to fetch {city}: {resp.status_code}")
    return None


def extract_data(**context):
    """Fetch data for all northern cities."""
    records = []
    for city in CITIES:
        record = fetch_city_data(city)
        if record:
            records.append(record)
    context['ti'].xcom_push(key='records', value=records)


def transform_data(**context):
    """Convert raw records to DataFrame and clean."""
    records = context['ti'].xcom_pull(key='records', task_ids='extract_data')
    if not records:
        raise ValueError("No records fetched!")
    df = pd.DataFrame(records)
    df["fetch_time"] = datetime.now().isoformat()
    context['ti'].xcom_push(key='df_json', value=df.to_json(orient="records"))


def load_data(**context):
    """Save data to CSV file."""
    df_json = context['ti'].xcom_pull(key='df_json', task_ids='transform_data')
    df = pd.read_json(df_json)
    filename = f"{DATA_DIR}/air_quality_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    df.to_csv(filename, index=False)
    print(f"✅ Data saved to {filename}")


# ─────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="air_quality_northern_thailand_dag",
    default_args=default_args,
    description="Fetch daily air quality data for Northern Thailand cities",
    schedule_interval="@daily",
    start_date=datetime(2025, 10, 30),
    catchup=False,
    tags=["ETL", "air_quality", "northern_thailand"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        provide_context=True
    )

    extract_task >> transform_task >> load_task
