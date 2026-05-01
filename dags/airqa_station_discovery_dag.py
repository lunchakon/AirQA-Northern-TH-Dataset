from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


API_TOKEN = os.getenv("AQICN_API_TOKEN")
DATA_DIR = Path("/opt/airflow/data")
STATION_CATALOG_PATH = DATA_DIR / "airqa_station_catalog.csv"
SEARCH_KEYWORDS = ["Chiang Mai", "Chiangmai", "Chiang Rai", "Chiangrai"]
PROVINCE_MATCHERS = {
    "Chiang Mai": ["chiang mai", "chiangmai"],
    "Chiang Rai": ["chiang rai", "chiangrai"],
}

FOCUS_AREAS = {
    "Chiang Mai": {
        "min_lat": 17.2,
        "min_lon": 98.0,
        "max_lat": 20.2,
        "max_lon": 99.6,
    },
    "Chiang Rai": {
        "min_lat": 19.0,
        "min_lon": 99.2,
        "max_lat": 20.6,
        "max_lon": 100.7,
    },
}


def _get_json(url: str, params: dict[str, str], timeout: int = 30) -> dict:
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") != "ok":
        raise ValueError(f"AQICN request failed: {payload}")
    return payload


def _classify_focus_area(*values: object) -> str | None:
    text = " ".join(str(value).lower() for value in values if pd.notna(value))
    for province, needles in PROVINCE_MATCHERS.items():
        if any(needle in text for needle in needles):
            return province
    return None


def _station_row_from_payload(station: dict, province_hint: str | None, source: str, fetched_at: str) -> dict:
    station_info = station.get("station", {})
    geo = station_info.get("geo") or [station.get("lat"), station.get("lon")]
    station_name = station_info.get("name")
    station_url = station_info.get("url")
    province = _classify_focus_area(station_name, station_url)
    if not province and source == "search":
        province = _classify_focus_area(province_hint)
    if not province:
        return {}

    return {
        "uid": station.get("uid"),
        "province_focus": province,
        "station_name": station_name,
        "station_url": station_url,
        "latitude": geo[0] if len(geo) > 0 else None,
        "longitude": geo[1] if len(geo) > 1 else None,
        "latest_aqi_from_bounds": station.get("aqi"),
        "latest_time_from_bounds": station.get("time"),
        "discovery_source": source,
        "discovered_at": fetched_at,
    }


def discover_stations() -> str:
    if not API_TOKEN:
        raise ValueError("AQICN_API_TOKEN is not set")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    fetched_at = datetime.utcnow().isoformat()

    for province, bounds in FOCUS_AREAS.items():
        latlng = "{min_lat},{min_lon},{max_lat},{max_lon}".format(**bounds)
        payload = _get_json(
            "https://api.waqi.info/map/bounds/",
            {"token": API_TOKEN, "latlng": latlng},
        )

        for station in payload.get("data", []):
            row = _station_row_from_payload(station, province, "bounds", fetched_at)
            if row:
                rows.append(row)

    for keyword in SEARCH_KEYWORDS:
        payload = _get_json(
            "https://api.waqi.info/search/",
            {"token": API_TOKEN, "keyword": keyword},
        )
        for station in payload.get("data", []):
            row = _station_row_from_payload(station, keyword, "search", fetched_at)
            if row:
                rows.append(row)

    catalog = pd.DataFrame(rows)
    if catalog.empty:
        raise ValueError("No AQICN stations found for Chiang Mai or Chiang Rai")

    catalog = (
        catalog.dropna(subset=["uid"])
        .drop_duplicates(subset=["uid"])
        .sort_values(["province_focus", "station_name"])
            )
    catalog.to_csv(STATION_CATALOG_PATH, index=False)
    return str(STATION_CATALOG_PATH)


default_args = {
    "owner": "airqa",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="airqa_station_discovery_dag",
    default_args=default_args,
    description="Discover AQICN stations around Chiang Mai and Chiang Rai",
    schedule_interval="@daily",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["air_quality", "aqicn", "station_catalog"],
) as dag:
    discover = PythonOperator(
        task_id="discover_stations",
        python_callable=discover_stations,
    )
