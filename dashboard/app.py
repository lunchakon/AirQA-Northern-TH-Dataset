from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st


DATA_DIR = Path("/app/data")
POLLUTANT_COLUMNS = ["pm25", "pm10", "o3", "no2", "so2", "co"]
WEATHER_COLUMNS = ["weather_t", "weather_h", "weather_p", "weather_w", "weather_wg"]
NEW_OBSERVATION_PATTERN = "airqa_station_observations_*.csv"
FORECAST_PATTERN = "airqa_station_forecasts_*.csv"
LEGACY_OBSERVATION_PATTERN = "air_quality_*.csv"
AQI_BANDS = [
    (50, "Good", "#2e7d32"),
    (100, "Moderate", "#f9a825"),
    (150, "Unhealthy for Sensitive Groups", "#ef6c00"),
    (200, "Unhealthy", "#c62828"),
    (300, "Very Unhealthy", "#6a1b9a"),
    (500, "Hazardous", "#4e342e"),
]


st.set_page_config(
    page_title="Northern Thailand Air Quality",
    page_icon="",
    layout="wide",
)


@st.cache_data(ttl=60)
def load_air_quality_data() -> pd.DataFrame:
    files = sorted(DATA_DIR.glob(NEW_OBSERVATION_PATTERN))
    dataset_type = "station"
    if not files:
        files = sorted(DATA_DIR.glob(LEGACY_OBSERVATION_PATTERN))
        dataset_type = "legacy_city"
    if not files:
        return pd.DataFrame()

    frames = []
    for file_path in files:
        frame = pd.read_csv(file_path)
        frame["source_file"] = file_path.name
        frames.append(frame)

    data = pd.concat(frames, ignore_index=True)
    if "dominant_pollutant" not in data.columns and "dominentpol" in data.columns:
        data["dominant_pollutant"] = data["dominentpol"]
    if "fetched_at" not in data.columns and "fetch_time" in data.columns:
        data["fetched_at"] = data["fetch_time"]
    if "measurement_time" not in data.columns and "time" in data.columns:
        data["measurement_time"] = data["time"]
    if "station_name" not in data.columns and "city" in data.columns:
        data["station_name"] = data["city"]
    if "province_focus" not in data.columns:
        data["province_focus"] = "Northern Thailand"
    if "latitude" not in data.columns:
        data["latitude"] = pd.NA
    if "longitude" not in data.columns:
        data["longitude"] = pd.NA

    data["dataset_type"] = dataset_type
    for column in ["aqi", "latitude", "longitude", *POLLUTANT_COLUMNS, *WEATHER_COLUMNS]:
        if column in data.columns:
            data[column] = pd.to_numeric(data[column], errors="coerce")
    data["measurement_time"] = pd.to_datetime(data.get("measurement_time"), errors="coerce")
    data["fetched_at"] = pd.to_datetime(data.get("fetched_at"), errors="coerce")
    return data.dropna(subset=["station_name", "aqi", "fetched_at"])


@st.cache_data(ttl=60)
def load_forecast_data() -> pd.DataFrame:
    files = sorted(DATA_DIR.glob(FORECAST_PATTERN))
    if not files:
        return pd.DataFrame()

    frames = []
    for file_path in files:
        frame = pd.read_csv(file_path)
        frame["source_file"] = file_path.name
        frames.append(frame)

    data = pd.concat(frames, ignore_index=True)
    for column in ["forecast_min", "forecast_avg", "forecast_max"]:
        data[column] = pd.to_numeric(data[column], errors="coerce")
    data["fetched_at"] = pd.to_datetime(data.get("fetched_at"), errors="coerce")
    data["forecast_day"] = pd.to_datetime(data.get("forecast_day"), errors="coerce")
    return data.dropna(subset=["station_name", "pollutant", "forecast_day", "fetched_at"])


def aqi_status(value: float) -> tuple[str, str]:
    for upper_bound, label, color in AQI_BANDS:
        if value <= upper_bound:
            return label, color
    return "Beyond AQI Scale", "#212121"


def latest_snapshot(data: pd.DataFrame) -> pd.DataFrame:
    latest_fetch = data["fetched_at"].max()
    return data[data["fetched_at"] == latest_fetch].copy()


def aqi_color(value: float) -> list[int]:
    _, color = aqi_status(value)
    color = color.lstrip("#")
    return [int(color[index : index + 2], 16) for index in (0, 2, 4)] + [190]


data = load_air_quality_data()
forecast_data = load_forecast_data()

st.title("Northern Thailand Air Quality")
st.caption("Dashboard reads AQICN CSV snapshots from the shared `data/` folder.")

if data.empty:
    st.info("No CSV files found yet. Run the Airflow DAG to create files in the data folder.")
    st.stop()

latest = latest_snapshot(data)
available_provinces = sorted(data["province_focus"].dropna().unique())
available_stations = sorted(data["station_name"].dropna().unique())

with st.sidebar:
    st.header("Filters")
    selected_provinces = st.multiselect(
        "Focus area",
        available_provinces,
        default=available_provinces,
    )
    selected_stations = st.multiselect(
        "Stations",
        available_stations,
        default=available_stations,
    )
    selected_metric = st.selectbox(
        "Trend metric",
        ["aqi", *[column for column in POLLUTANT_COLUMNS if column in data.columns]],
    )
    show_raw = st.toggle("Show raw latest rows", value=False)

if selected_provinces:
    data = data[data["province_focus"].isin(selected_provinces)]
    latest = latest[latest["province_focus"].isin(selected_provinces)]
    if not forecast_data.empty:
        forecast_data = forecast_data[forecast_data["province_focus"].isin(selected_provinces)]
if selected_stations:
    data = data[data["station_name"].isin(selected_stations)]
    latest = latest[latest["station_name"].isin(selected_stations)]
    if not forecast_data.empty:
        forecast_data = forecast_data[forecast_data["station_name"].isin(selected_stations)]

if latest.empty:
    st.warning("No rows match the selected filters.")
    st.stop()

average_aqi = latest["aqi"].mean()
max_row = latest.loc[latest["aqi"].idxmax()]
min_row = latest.loc[latest["aqi"].idxmin()]
status_label, status_color = aqi_status(float(average_aqi))

st.markdown(
    f"""
    <style>
    .aqi-pill {{
        display: inline-block;
        padding: 0.25rem 0.65rem;
        border-radius: 999px;
        color: white;
        background: {status_color};
        font-weight: 700;
        font-size: 0.9rem;
    }}
    div[data-testid="stMetric"] {{
        background: #f7f9fb;
        border: 1px solid #e5e7eb;
        border-radius: 8px;
        padding: 0.85rem 1rem;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(f'<span class="aqi-pill">{status_label}</span>', unsafe_allow_html=True)

metric_columns = st.columns(4)
metric_columns[0].metric("Latest average AQI", f"{average_aqi:.0f}")
metric_columns[1].metric("Highest station", str(max_row["station_name"]), f"AQI {max_row['aqi']:.0f}")
metric_columns[2].metric("Lowest station", str(min_row["station_name"]), f"AQI {min_row['aqi']:.0f}")
metric_columns[3].metric(
    "Last refresh",
    latest["fetched_at"].max().strftime("%Y-%m-%d %H:%M"),
)

st.subheader("Station Map")
map_data = latest.dropna(subset=["latitude", "longitude"]).copy()
if map_data.empty:
    st.info("No station coordinates are available in the current CSV files yet.")
else:
    map_data["aqi_color"] = map_data["aqi"].apply(aqi_color)
    map_data["label"] = map_data.apply(
        lambda row: f"{row['station_name']} | AQI {row['aqi']:.0f}",
        axis=1,
    )
    view_state = pdk.ViewState(
        latitude=float(map_data["latitude"].mean()),
        longitude=float(map_data["longitude"].mean()),
        zoom=7,
        pitch=0,
    )
    st.pydeck_chart(
        pdk.Deck(
            map_style=None,
            initial_view_state=view_state,
            layers=[
                pdk.Layer(
                    "ScatterplotLayer",
                    data=map_data,
                    get_position="[longitude, latitude]",
                    get_fill_color="aqi_color",
                    get_radius=4500,
                    radius_min_pixels=6,
                    radius_max_pixels=18,
                    pickable=True,
                )
            ],
            tooltip={"text": "{label}\n{province_focus}\n{dominant_pollutant}"},
        ),
        height=420,
    )
    st.caption("Map data attribution: World Air Quality Index Project and originating EPA where provided.")

st.subheader("Latest AQI by Station")
latest_chart_data = latest.sort_values("aqi", ascending=False).set_index("station_name")
st.bar_chart(latest_chart_data[["aqi"]], color="#d97706", height=320)

left_column, right_column = st.columns([1.1, 0.9])

with left_column:
    st.subheader("Trend")
    trend = (
        data.groupby(["fetched_at", "station_name"], as_index=False)[selected_metric]
        .mean()
        .sort_values("fetched_at")
    )
    st.line_chart(
        trend,
        x="fetched_at",
        y=selected_metric,
        color="station_name",
        height=320,
    )

with right_column:
    st.subheader("Pollutants")
    pollutant_columns = [column for column in POLLUTANT_COLUMNS if column in latest.columns]
    pollutant_data = latest[["station_name", *pollutant_columns]].set_index("station_name")
    st.dataframe(
        pollutant_data.style.format("{:.1f}", na_rep="-"),
        use_container_width=True,
        height=320,
    )

if not forecast_data.empty:
    st.subheader("Forecast")
    latest_forecast_time = forecast_data["fetched_at"].max()
    latest_forecast = forecast_data[forecast_data["fetched_at"] == latest_forecast_time]
    forecast_pollutants = sorted(latest_forecast["pollutant"].dropna().unique())
    forecast_pollutant = st.selectbox("Forecast pollutant", forecast_pollutants, index=0)
    forecast_view = latest_forecast[latest_forecast["pollutant"] == forecast_pollutant]
    forecast_view = (
        forecast_view.groupby(["forecast_day", "province_focus"], as_index=False)["forecast_avg"]
        .mean()
        .sort_values("forecast_day")
    )
    st.line_chart(
        forecast_view,
        x="forecast_day",
        y="forecast_avg",
        color="province_focus",
        height=260,
    )

st.subheader("Ranked Latest Readings")
rank_columns = [
    "province_focus",
    "station_name",
    "aqi",
    "dominant_pollutant",
    "measurement_time",
    "fetched_at",
    "latitude",
    "longitude",
    *POLLUTANT_COLUMNS,
]
rank_columns = [column for column in rank_columns if column in latest.columns]
ranked = latest.sort_values("aqi", ascending=False)[rank_columns]
st.dataframe(
    ranked,
    use_container_width=True,
    hide_index=True,
    column_config={
        "aqi": st.column_config.ProgressColumn(
            "AQI",
            min_value=0,
            max_value=300,
            format="%d",
        ),
        "dominant_pollutant": "Dominant pollutant",
    },
)

if show_raw:
    st.subheader("Raw Latest Rows")
    st.dataframe(latest, use_container_width=True, hide_index=True)
