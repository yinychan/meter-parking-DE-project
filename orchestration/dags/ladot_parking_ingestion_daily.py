from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
import requests
import os
import pandas as pd
import boto3
from io import StringIO, BytesIO
from sodapy import Socrata
from itertools import count
import json

class IngestionEngine:
    """
    This class will contain all of our ingestion logic. 
    We can have different methods for each data source, 
    and then call those methods in our DAG.
    """
    def __init__(self, dataset_name: str, dataset_id: str):
        self.app_token = os.getenv("APP_TOKEN")
        self.s3_bucket = os.getenv("AWS_S3_BUCKET")
        self.s3_client = boto3.client("s3")

        self.dataset_id = dataset_id
        self.dataset_name = dataset_name

        self.socrata_client = Socrata("data.lacity.org", self.app_token, timeout=120)
    
    def extract_and_load_data(self, chunk_size: int = 20000):
        #1. set up variables to insert into request
        current_date = datetime.now().strftime('%Y%m%d_%H%M%S')

        print(f"Starting ingestion for {self.dataset_name}")

        #2. Iterate until the dataset has no more rows, loading each chunk into S3 individually.
        try:
            for i in count(start=0):
                offset = i * chunk_size

                chunked = self.socrata_client.get(
                    self.dataset_id, 
                    limit=chunk_size,
                    offset=offset,
                    order=":id",
                    where="issue_date >= '2021-01-01T00:00:00'" if self.dataset_name == "parking_citations" else None
                )

                print(f"Fetched rows {offset} to {offset + chunk_size} in {self.dataset_name}")
                
                if not chunked or len(chunked) == 0:
                    break

                self._upload_chunk_to_s3(chunked, current_date, i)

        except Exception as e:
            print(f"Error interruption while ingesting {self.dataset_name}: {e}")
            raise


    def _upload_chunk_to_s3(self, records: list, timestamp: str, chunk_idx: int):
        
        s3_filename = f"{self.dataset_name}/run_{timestamp}_chunk_{chunk_idx:05d}.parquet"

        #3. convert to dataframe with datatypes that won't break db injection
        df_chunk = pd.DataFrame.from_records(records)
        df_chunk = df_chunk.map(
            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x # convert any nested dictionaries or lists to JSON strings to avoid issues when inserting into the database
        )

        #4. write to parquet in memory
        parquet_buffer = BytesIO()
        df_chunk.to_parquet(parquet_buffer, index=False, engine="pyarrow", compression="snappy")

        #5. stream parquet file to S3
        try:
            response = self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_filename,
                Body=parquet_buffer.getvalue()
            )
            print(f"Successfully uploaded {s3_filename} to S3 bucket {self.s3_bucket}")
        except Exception as e:
            print(f"Error uploading {s3_filename} to S3 bucket {self.s3_bucket}: {e}")
            raise

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
        Instantiate and run the data extraction 
        and loading for the occupancy data stream
        """

        engine = IngestionEngine(
            dataset_name="meter_occupancy",
            dataset_id="e7h6-4a3e"
        )
        engine.extract_and_load_data()

    @task(task_id="ingest_parking_citations_data")
    def extract_and_load_parking_citations_data():
        """
        Instantiate and run the data extraction 
        and loading for the parking citations data stream
        """
        engine = IngestionEngine(
            dataset_name="parking_citations",
            dataset_id="4f5p-udkv"
        )
        engine.extract_and_load_data()

    @task(task_id="ingest_parking_inventory_policies_data")
    def extract_and_load_parking_inventory_policies_data():
        """
        Instantiate and run the data extraction 
        and loading for the parking inventory & policies data stream
        """
        engine = IngestionEngine(
            dataset_name="parking_inventory_policies",
            dataset_id="s49e-q6j2"
        )
        engine.extract_and_load_data()

    trigger_aws_glue_crawler = GlueCrawlerOperator(
        task_id="trigger_aws_glue_crawler",
        aws_conn_id="aws_default",
        wait_for_completion=True, 
        poll_interval=15,
        config={
            "Name": "ladot_parking_metrics_crawler"
        }
    )

    # Instantiate the task(s)
    run_meter_occupancy_data = extract_and_load_meter_occupancy_data()
    run_parking_citations_data = extract_and_load_parking_citations_data()
    run_parking_inventory_policies_data = extract_and_load_parking_inventory_policies_data()

    # Airflow will automatically set these tasks to run one after the other
    run_meter_occupancy_data >> run_parking_inventory_policies_data >> run_parking_citations_data >> trigger_aws_glue_crawler
    # [run_meter_occupancy_data, run_parking_citations_data, run_parking_inventory_policies_data] # run in parallel

# Instantiate DAG
ladot_dag = ladot_parking_ingestion_daily()