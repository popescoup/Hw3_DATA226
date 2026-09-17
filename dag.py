from datetime import datetime

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


SNOWFLAKE_CONN_ID = "snowflake_conn"
TARGET_TABLE = "HOMEWORK_2.RAW.WEATHER_DATA_HW"


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract():
    latitude = float(Variable.get("LATITUDE"))
    longitude = float(Variable.get("LONGITUDE"))

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": 60,
        "forecast_days": 0,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code",
        ],
        "timezone": "America/Los_Angeles",
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


@task
def transform(data):
    daily = data["daily"]
    records = []
    for i, date in enumerate(daily["time"]):
        records.append([
            data["latitude"],
            data["longitude"],
            date,
            daily["temperature_2m_max"][i],
            daily["temperature_2m_min"][i],
            daily["precipitation_sum"][i],
            daily["weather_code"][i],
        ])
    print(f"Transformed {len(records)} records")
    return records


@task
def load(records, target_table):
    cur = return_snowflake_conn()
    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                latitude FLOAT,
                longitude FLOAT,
                date DATE,
                temp_max FLOAT,
                temp_min FLOAT,
                precipitation FLOAT,
                weather_code INT,
                PRIMARY KEY (latitude, longitude, date)
            )
        """)

        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {target_table}")
        for r in records:
            cur.execute(
                f"""INSERT INTO {target_table}
                    (latitude, longitude, date, temp_max, temp_min, precipitation, weather_code)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                r,
            )
        cur.execute("COMMIT;")
        print(f"Loaded {len(records)} records into {target_table}")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise
    finally:
        cur.close()


with DAG(
    dag_id="WeatherData_ETL",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["ETL", "weather"],
    schedule="30 2 * * *",
) as dag:
    raw_data = extract()
    rows = transform(raw_data)
    load(rows, TARGET_TABLE)