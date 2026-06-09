from datetime import datetime, timedelta
from airflow.decorators import dag, task

import requests
import os
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from tqdm.auto import tqdm

@dag(
    dag_id="ladot_parking_ingestion_daily",
    start_date=datetime(2026, 6, 8), # set to a past date to allow immediate execution
    catchup=False, # don't backfill missed runs
    schedule=timedelta(days=1), # use timedelta for daily schedule precision
    default_args={
        "owner": "airflow",
        "depends_on_past": False, # if yesterday's run fails, it won't block today's run
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def ladot_parking_ingestion_daily():
    """
    This is the main ingestion function. 
    We will call the individual tasks for each data source here.
    """
    @task(task_id="ingest_meter_occupancy_data")
    def extract_and_load_meter_occupancy_data():
        """
        We're taking our raw ingestion logic from pipeline.py and putting it here:
        """

        #1. set up variables to insert into request
        app_token = os.getenv("APP_TOKEN")

        headers = {
            "X-App-Token": app_token,
            "Content-Type": "application/json",
        }
        payload_csv = {
            "query": "SELECT *",
            "orderingSpecifier": "discard"
        }
        meter_occupancy_csv = "https://data.lacity.org/api/v3/views/e7h6-4a3e/export.csv"

        #2. retrieve data from API
        meter_occupancy_response = requests.post(meter_occupancy_csv, headers=headers, json=payload_csv, timeout=100)
        meter_occupancy_response.raise_for_status() # Check if the request was successful

        #3. put response data into dataframe
        # df = pd.read_csv(StringIO(meter_occupancy_response.text))

        #4. Let's make sure we have the data we expect
        # print(df.head())
        # print(df.dtypes)
        # print(df.shape)
        # print(df.columns)

        #5. connect to the database
        psql_user = os.getenv("POSTGRES_USER")
        psql_password = os.getenv("POSTGRES_PASSWORD")
        psql_db = os.getenv("POSTGRES_DB")
        psql_port = os.getenv("POSTGRES_PORT", "5432") # default to 5432 if not set
        psql_host = os.getenv("POSTGRES_HOST", "postgres") # default to 'postgres' if not set (because we're in Docker)

        engine = create_engine(
            f"postgresql://{psql_user}:{psql_password}@{psql_host}:{psql_port}/{psql_db}"
        )

        #6. inserting data
        df_iterable = pd.read_csv(
            StringIO(meter_occupancy_response.text),
            iterator=True,
            chunksize=100
        )

        first = True

        for df_chunk in tqdm(df_iterable):

            if first:
                # Create table schema (no data)
                df_chunk.head(0).to_sql(
                    name="meter_occupancy",
                    con=engine,
                    if_exists="replace"
                )
                first = False
                print("Table created")

            # Insert chunk
            df_chunk.to_sql(
                name="meter_occupancy",
                con=engine,
                if_exists="append"
            )

            print("Inserted:", len(df_chunk))

    # Instantiate the task(s)
    run_meter_occupancy_data = extract_and_load_meter_occupancy_data()

# Instantiate DAG
ladot_dag = ladot_parking_ingestion_daily()