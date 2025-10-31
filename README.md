# AirQA-Northern-TH-Dataset
This project focuses on creating and analyzing air quality data for the Northern region of Thailand

### Data Acquisition:
- utilize the Air Quality Index China (AQICN) API, a publicly available resource (https://aqicn.org/api/). This API provides real-time air quality data for various locations across the globe

### Data Focus: 
- Specifically target the Northern region of Thailand. By filtering the retrieved data through the API, we will concentrate on air quality measurements within this specific geographic area.

### Data Analysis:
- I will leverage the power of Python libraries like pandas for data manipulation and analysis.Jupyter Notebook will serve as our development environment, facilitating interactive exploration and visualization of the air quality data.


### Trend Analysis: 
-  Through data analysis techniques, we aim to identify trends and patterns in the air quality data for the Northern region of Thailand. This analysis can provide valuable insights into the region's air quality status and potential seasonal variations.


### Project Output: 
- This project will result in the creation of an Air Quality Dataset for the Northern Region of Thailand (AirQA-Northern-TH-Dataset). This dataset will be stored in a structured format, potentially a CSV file, for future reference and potential use in further analysis or machine learning models.

### Benefits: 
The AirQA-Northern-TH-Dataset can be utilized for various purposes:
- Public Awareness: Disseminating air quality information can empower individuals to make informed health decisions, particularly regarding outdoor activities.
- Environmental Monitoring: The dataset can contribute to broader environmental monitoring efforts, enabling analysis of long-term air quality trends in the region.
- Policy Development: The data can be used to support policy development aimed at improving air quality in the Northern region of Thailand.
---------------



### Real-Time Air Quality ETL Pipeline for Northern Thailand

This project builds an **ETL (Extract-Transform-Load)** data pipeline using **Apache Airflow**, designed to collect, process, and analyze **air quality data** from the **Northern region of Thailand**.  
The data is sourced from the [AQICN API](https://aqicn.org/api/) — a public service providing real-time air quality information from thousands of monitoring stations worldwide.

---

## 🧭 Project Overview

**Objective:**  
Create an automated daily pipeline that retrieves AQI (Air Quality Index) data, filters for key provinces in Northern Thailand, transforms it into a structured format, and loads it into PostgreSQL for analysis or visualization.

**Key Features**
- 🔄 Automated ETL pipeline orchestrated by **Apache Airflow**
- 🌤 Data sourced from **AQICN Public API**
- 🧹 Data transformation using **Python + Pandas**
- 🗃 Persistent storage in **PostgreSQL**
- 📈 Ready for dashboards (Streamlit, Grafana, or Jupyter Notebook)
- 📁 Exports clean **CSV datasets** for public or ML use

---

## 🏗️ Project Architecture

```bash
airqa-northern-th/
├── dags/
│   └── airqa_etl_dag.py         # Airflow DAG for daily ETL
├── scripts/
│   └── transform_air_data.py    # Custom transformation logic
├── requirements.txt             # Python dependencies
├── docker-compose.yaml          # Airflow + PostgreSQL stack
├── .env                         # API keys & environment variables
├── airflow/
│   ├── logs/                    # Task logs
│   └── config/                  # Optional custom Airflow configs
└── README.md
