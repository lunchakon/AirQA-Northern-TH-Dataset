from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


API_TOKEN = os.getenv("AQICN_API_TOKEN")
DATA_DIR = Path("/opt/airflow/data")
STATION_CATALOG_PATH = DATA_DIR / "airqa_station_catalog.csv"
OBSERVATION_PREFIX = "airqa_station_observations"
FORECAST_PREFIX = "airqa_station_forecasts"
SEARCH_KEYWORDS = ["Chiang Mai", "Chiangmai", "Chiang Rai", "Chiangrai"]
PROVINCE_MATCHERS = {
    "Chiang Mai": ["chiang mai", "chiangmai"],
    "Chiang Rai": ["chiang rai", "chiangrai"],
}

POLLUTANTS = ["pm25", "pm10", "o3", "no2", "so2", "co"]
WEATHER_FIELDS = ["t", "h", "p", "w", "wg"]
FORECAST_FIELDS = ["pm25", "pm10", "o3", "uvi"]

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


def _discover_station_catalog() -> pd.DataFrame:
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
    return pd.DataFrame(rows).dropna(subset=["uid"]).drop_duplicates(subset=["uid"])


def _load_catalog() -> pd.DataFrame:
    if STATION_CATALOG_PATH.exists():
        catalog = pd.read_csv(STATION_CATALOG_PATH)
        if not catalog.empty:
            return catalog

    catalog = _discover_station_catalog()
    if catalog.empty:
        raise ValueError("No AQICN stations found for Chiang Mai or Chiang Rai bounds")
    catalog.to_csv(STATION_CATALOG_PATH, index=False)
    return catalog


def _extract_iaqi_value(data: dict[str, Any], key: str) -> Any:
    value = data.get("iaqi", {}).get(key, {})
    if isinstance(value, dict):
        return value.get("v")
    return None


def _extract_attribution(data: dict[str, Any]) -> str:
    attributions = data.get("attributions") or []
    names = [item.get("name") for item in attributions if item.get("name")]
    return "; ".join(names)


def _build_observation_row(station_row: pd.Series, data: dict[str, Any], fetched_at: str) -> dict[str, Any]:
    city = data.get("city", {})
    province = _classify_focus_area(
        city.get("name"),
        city.get("url"),
        station_row.get("station_name"),
        station_row.get("station_url"),
    )
    if not province and station_row.get("discovery_source") == "search":
        province = _classify_focus_area(station_row.get("province_focus"))
    if not province:
        return {}

    row = {
        "fetched_at": fetched_at,
        "uid": station_row.get("uid"),
        "province_focus": province,
        "station_name": city.get("name") or station_row.get("station_name"),
        "station_url": city.get("url") or station_row.get("station_url"),
        "latitude": (city.get("geo") or [station_row.get("latitude"), None])[0],
        "longitude": (city.get("geo") or [None, station_row.get("longitude")])[1],
        "aqi": data.get("aqi"),
        "idx": data.get("idx"),
        "dominant_pollutant": data.get("dominentpol"),
        "measurement_time": data.get("time", {}).get("s"),
        "measurement_tz": data.get("time", {}).get("tz"),
        "source_attribution": _extract_attribution(data),
    }

    for pollutant in POLLUTANTS:
        row[pollutant] = _extract_iaqi_value(data, pollutant)
    for field in WEATHER_FIELDS:
        row[f"weather_{field}"] = _extract_iaqi_value(data, field)
    return row


def _build_forecast_rows(station_row: pd.Series, data: dict[str, Any], fetched_at: str) -> list[dict[str, Any]]:
    forecast = data.get("forecast", {}).get("daily", {})
    province = _classify_focus_area(
        data.get("city", {}).get("name"),
        data.get("city", {}).get("url"),
        station_row.get("station_name"),
        station_row.get("station_url"),
    )
    if not province and station_row.get("discovery_source") == "search":
        province = _classify_focus_area(station_row.get("province_focus"))
    if not province:
        return []

    rows = []
    for pollutant in FORECAST_FIELDS:
        for item in forecast.get(pollutant, []):
            rows.append(
                {
                    "fetched_at": fetched_at,
                    "uid": station_row.get("uid"),
                    "province_focus": province,
                    "station_name": data.get("city", {}).get("name") or station_row.get("station_name"),
                    "pollutant": pollutant,
                    "forecast_day": item.get("day"),
                    "forecast_min": item.get("min"),
                    "forecast_avg": item.get("avg"),
                    "forecast_max": item.get("max"),
                }
            )
    return rows


def capture_station_observations() -> dict[str, str]:
    if not API_TOKEN:
        raise ValueError("AQICN_API_TOKEN is not set")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    catalog = _load_catalog()
    fetched_at = datetime.utcnow().isoformat()
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    observation_rows = []
    forecast_rows = []
    for _, station_row in catalog.iterrows():
        uid = str(station_row["uid"])
        payload = _get_json(f"https://api.waqi.info/feed/@{uid}/", {"token": API_TOKEN})
        station_data = payload.get("data", {})
        observation_row = _build_observation_row(station_row, station_data, fetched_at)
        if observation_row:
            observation_rows.append(observation_row)
        forecast_rows.extend(_build_forecast_rows(station_row, station_data, fetched_at))

    observations = pd.DataFrame(observation_rows)
    forecasts = pd.DataFrame(forecast_rows)
    if observations.empty:
        raise ValueError("No matching Chiang Mai or Chiang Rai station observations were captured")
    observation_path = DATA_DIR / f"{OBSERVATION_PREFIX}_{timestamp}.csv"
    forecast_path = DATA_DIR / f"{FORECAST_PREFIX}_{timestamp}.csv"
    observations.to_csv(observation_path, index=False)
    forecasts.to_csv(forecast_path, index=False)
    return {"observations": str(observation_path), "forecasts": str(forecast_path)}


default_args = {
    "owner": "airqa",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="airqa_station_observations_dag",
    default_args=default_args,
    description="Capture detailed AQICN station observations every three hours",
    schedule_interval="0 */3 * * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["air_quality", "aqicn", "station_observations"],
) as dag:
    capture = PythonOperator(
        task_id="capture_station_observations",
        python_callable=capture_station_observations,
    )
