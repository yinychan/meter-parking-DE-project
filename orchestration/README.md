## Date Workflow Orchestration
We will be setting up Apache Airflow using Docker and working with AWS. Airflow acts as a "centralized control room" for our data engineering pipelines. Instead of running Python extraction scripts on a manual timer or using a cron job, Airflow orchestrates the entire data workflow. 

## Contents

- [Airflow & DAGs](#airflow--dags)
    - [Getting Started](#getting-started)
    - [Setting up Airflow](#setting-up-airflow)
    - [Airflow in Docker](#airflow-in-docker)
    - [Docker Compose for Airflow](#docker-compose-for-airflow)
    - [Build Docker Image with Airflow](#build-docker-image-with-airflow)
    - [DAGs](#dags)
    - [Running your DAG](#running-your-dag)

- [Cloud Data Lake with AWS](#cloud-data-lake-with-aws)
    - [Setting Up](#setting-up)
    - [Modify DAG for AWS S3 Ingestion](#modify-dag-for-aws-s3-ingestion)
    - [Together with Terraform](#together-with-terraform)
    - [Extract & Load All Datasets](#extract--load-all-datasets)
        - [Using Socrata API](#using-socrata-api)
    - [AWS Glue Crawler](#aws-glue-crawler) (coming up next)

## Airflow & DAGs

### Getting Started

This setup works with Apache Airflow 3.2.2. If you are using a different Airflow version, especially from Apache 2.x, there will be a number of differences in your docker-compose.yml.  

Specifically, pay attention to whether you need `AIRFLOW__CORE__EXECUTOR` to equal `LocalExecutor`, `CeleryExecutor`, or other. If using `LocalExecutor`, you'll need to ensure you have these configs in your `docker-compose.yml` with the corresponding values in your `.env`:

```
AIRFLOW__CORE__EXECUTION_API_SERVER_URL: 'http://airflow-apiserver:8080/execution/'
AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW__API_AUTH__JWT_SECRET:-airflow_jwt_secret}
AIRFLOW__API_AUTH__JWT_ISSUER: ${AIRFLOW__API_AUTH__JWT_ISSUER:-airflow}
```

Create an `orchestration` folder in your project.

```
ls 
# make sure you're in your project root

mkdir orchestration
cd orchestration
```

Locate the `accessKeys.csv` you downloaded in the [terraform exercise](../terraform/README.md). You can refer back to the AWS IAM user setup.

### Setting up Airflow

We want to be able to modify and save changes to our DAG files, so to avoid permissions issues with the default root user setup, we need to pass in our unique host user ID and set the group ID to 0.

```
# In your orchestration directory,

mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" >> .env
```

### Airflow in Docker

We will use `requirements.txt` to layer caching. We will instruct Docker to copy `requirements.txt` and run `uv pip install` before running `COPY . .` so it caches heavy library installations.

```
# In your orchestration directory

touch Dockerfile
touch requirements.txt
```

This is what we'll need in our `Dockerfile`:

```
# Use the latest Airflow image as the base image
FROM apache/airflow:3.2.2

# Copy your requirements file into place
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
```

In your `requirements.txt` file, we list all the external Python libraries and packages our project needs. AWS package is all we need for now:

```
# AWS Provider & SDK
apache-airflow-providers-amazon
boto3

# Data Compression & Serialization
pyarrow

# Core Database Driver required by SQLAlchemy to connect to Postgres 18
psycopg2-binary
```

### Docker Compose for Airflow

Copy the [lean docker-compose.yml I have for this project](./docker-compose.yml). You can also download one from Airflow, but it comes with more than 300 lines of code, a lot of which we won't need. You can fetch Airflow's docker-compose.yaml here:

```
# Run this if you want the full extensive version from Airflow. We won't need most of what it generates

curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.2.2/docker-compose.yaml'
```

Then, copy over the [.env.example](./.env.example) into your own `.env` file specific to this orchestration directory

```
ls

# Make sure you're in /project/orchestration

touch .env

# Copy / paste contents from my .env.example into your .env. 
```

From within your newly created `.env`, you'll need to replace the following values:

```
POSTGRES_DB= # come up with one relating to your project
AIRFLOW__CORE__FERNET_KEY= # follow the next set of instructions
AWS_ACCESS_KEY_ID= # from the same .csv created in the terraform exercise
AWS_SECRET_ACCESS_KEY= # from the same .csv created in the terraform exercise
APP_TOKEN= # generated from https://data.lacity.org/profile/edit/developer_settings
AIRFLOW__API_AUTH__JWT_SECRET= # run in your terminal: python3 -c "import secrets; print(secrets.token_hex(32))"
```
For your convenience, here's the link to [Generate an Access Key Pair](../terraform/README.md) in the Terraform exercise where you generated an `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

To generate your own Fernet key `AIRFLOW__CORE__FERNET_KEY`, run this in your terminal:

```
uv run --with cryptography python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Make sure you add your APP_TOKEN from the pipeline exercise, or [generate their API Token](../pipeline/README.md) to enter the `APP_TOKEN` value in your `.env` for this exercise.

```
# In /project/orchestration/.env, switch out the value for your actual token

APP_TOKEN=app_token_from_https://data.lacity.org/profile/edit/developer_settings
```

Generate an `AIRFLOW__API_AUTH__JWT_SECRET` value by running the following in your terminal and then copy/paste the generated secret:

```
python3 -c "import secrets; print(secrets.token_hex(32))"
```

### Build Docker Image with Airflow

We have our `Dockerfile`, `docker-compose.yml`, `.env`, and `requirements.txt`. We're ready to build

```
docker compose build # build the image
docker compose up airflow-init # initialize airflow scheduler
docker compose up # all other services in the container
```

#### Airflow Dashboard

Success means you should be able to access `https://localhost:8080` using the airflow/airflow username and password we set in .env.

### DAGs

A DAG (Directed Acyclic Graph) is the code representation of a data pipeline workflow. It is a collection of all the tasks you want to run organized in a way that reflects their relationships and dependencies.

Create your first DAG file

```
ls
cd dags/
# You need to be in your dags directory 
# e.g. /your-project-repo/orchestration/dags

touch ladot_parking_ingestion_daily_sql.py
```

1. Import Airflow directo decorators (`@dag` and `@task`) along with calendar tracking tools

```
# In ladot_parking_ingestion_daily_sql.py

from datetime import datetime, timedelta
from airflow.decorators import dag, task
```

2. Decorate your main wrapper function to define pipeline settings. I've gone ahead and commented on further descriptions of certain options we need.

```
@dag(
    dag_id="ladot_parking_ingestion_daily_sql",
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
```

3. Write out your main pipeline ingestion logic. To start, we copy the raw Python from `pipeline.py` in an [earlier pipeline exercise](../pipeline/README.md) and paste into our new piepline function

```
@dag(
    ...
)
def ladot_parking_ingestion_daily_sql():
    @task(task_id="ingest_meter_occupancy_data")
    def extract_and_load_meter_occupancy_data():
        # paste the code from pipeline.py here
        app_token = os.getenv("APP_TOKEN")
        headers = {
            ...
        }
        ...
        df_chunk.to_sql(
            ...
        )
        ...
```

Import our pipeline tools at the top of the file. Notice I removed `tqdm` from this script (to reduce complications when doing this test run of Airflow). Also removed the `tqdm` method call around Line 79.

```
# In ladot_parking_ingestion_daily_sql.py

import requests
import os
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
```

Note that we'll need to default our db host to `postgres` since we'll be running in Docker. 

```
# Line ~63 in ladot_parking_ingestion_daily_sql.py

psql_host = os.getenv("POSTGRES_HOST", "postgres")
```

And make sure `APP_TOKEN` is in your `docker-compose.yml` environment variables:

```
x-airflow-common: &airflow-common
  build:
    context: .
    dockerfile: Dockerfile
  environment:
    ...
    AIRFLOW__CORE__EXECUTION_API_SERVER_URL: 'http://airflow-apiserver:8080/execution/'
    AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW__API_AUTH__JWT_SECRET:-airflow_jwt_secret}
    AIRFLOW__API_AUTH__JWT_ISSUER: ${AIRFLOW__API_AUTH__JWT_ISSUER:-airflow}
    ...
    APP_TOKEN: ${APP_TOKEN}
    POSTGRES_USER: ${POSTGRES_USER}
    POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    POSTGRES_DB: ${POSTGRES_DB}
    ...
```

4. Instantiate the execution and call the main entrypoint function

```
    # Instantiate the task(s)
    run_meter_occupancy_data = extract_and_load_meter_occupancy_data()

# Instantiate DAG
ladot_dag = ladot_parking_ingestion_daily_sql()
```

5. Let's give it a go. Since we've updated the `.env` file, we'll ned to bring Docker down then back up again

```
docker compose down
docker compose build --no-cache
docker compose up
```

Reload your local Airflow dashboard `localhost:8080`. If necessary, enter your username / password: airflow / airflow.

If you don't see your DAG in the dashboard, you can manually rescan for it in a different terminal tab:

```
cd orchestration/
docker compose exec airflow-scheduler airflow dags reserialize
```

You should see your DAG `ladot_parking_ingestion_daily_sql` listed on your dash but with the trigger toggle disabled (this is default). Manually start your dag by clicking the `>` play.


6. Airflow commands:

You can run this command to show a list of dags in service

```
# List all the dags
docker compose exec airflow-scheduler airflow dags list
```

If your dag hasn't shown up yet, run this command to have your dag instantly show up in your dashboard without waiting.

```
# Force an immediate rescan (force the running scheduler to process newly fixed code in your dag file)
docker compose exec airflow-scheduler airflow dags reserialize
```

#### DAG in Airflow Dashboard

Success here means you can see your DAG within your `localhost:8080/dags` Airflow Dashboard (select the All tab).

### Running your DAG

You'll notice your DAG's auto-trigger toggle is in the "off" position. You can turn it on for it to run on the next scheduled time. But since we are simply running an example exercise, you can manually trigger the dag to test that it works. Click on the `>` play button to get it going. Select "Single Run" and have it start immediately.

Success here will quite literally show a green "success" checkmark ✅. You should also be able to see the `print()` outputs in your DAG's task logs within Airflow dashboard.

Let's check the database. In a new terminal tab:

```
ls
cd orchestration # if necessary
# make sure you're in the /your-project/orchestration directory first

docker compose exec postgres psql -U airflow -d la_meter_parking_db_airflow

# You should be in psql now, with something like `your_db_airflow=#`

SELECT * FROM meter_occupancy LIMIT 10;
```

Great! Our Airflow and DAG work, next, we take it into AWS.

## Cloud Data Lake with AWS

To summarize what we've done so far, we've set up Apache Airflow in Docker and successfully ran our first task with data ingestion into PostgreSQL on Docker.

We need to take it one step further and transition from our PostgreSQL set up into our Data Lake on AWS. We'll implement with the first dataset and then implement similar tasks for our last 2 datasets.

### Setting Up

We need to make sure we have the following essentials to move data onto AWS:

1. Python libraries `boto3` and `pyarrow`. `boto3` is the official AWS SDK for Python. `pyarrow` will have what we need to compress our data into high-performance Parquet files. First, let's update our `requirements.txt` file.

```
# In orchestration/requirements.txt

# AWS Provider & SDK
apache-airflow-providers-amazon
boto3

# Data Compression & Serialization
pyarrow

# Core Database Driver required by SQLAlchemy to connect to Postgres 18
psycopg2-binary
```

2. Make sure your `.env` and `docker-compose.yml` includes all the AWS credentials you'll need:

```
# In .env
AWS_ACCESS_KEY_ID=aws_access_key_id_example
AWS_SECRET_ACCESS_KEY=aws_secret_access_key_example
AWS_DEFAULT_REGION=us-west-1
AWS_S3_BUCKET=ladot-meter-parking-de-project-aws-data-lake # or your-project-name-aws-data-lake
```

```
# In docker-compose.yml
  environment:
    &airflow-common-env
    ...
    AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
    AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
    AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION}
    AWS_S3_BUCKET: ${AWS_S3_BUCKET}
```

With that update, we'll need to re-compile our Docker image layers. In your terminal:

```
ls
# make sure you're in your ./orchestration directory

docker compose down
docker compose build --no-cache
docker compose up
```

Reload your local Airflow dashboard `localhost:8080`. If necessary, enter your username / password: airflow / airflow.

If you don't see your DAG in the dashboard, you can manually rescan for it in a different terminal tab:

```
cd orchestration/
docker compose exec airflow-scheduler airflow dags reserialize
```

### Modify DAG for AWS S3 Ingestion

In the earlier sections, we were dealing with a DAG that saved our data into a local PostgreSQL setup on Docker. For this section, we'll work with the `ladot_parking_meter_ingestion_daily.py` file.  

```
ls
cd dags/
# You need to be in your dags directory 
# e.g. /your-project-repo/orchestration/dags

touch ladot_parking_meter_ingestion_daily.py
```

If you need a side-by-side of the before and after, diff between my versions of `ladot_parking_ingestion_daily_sql.py` and `ladot_parking_meter_ingestion_daily.py`.

Working with `ladot_parking_meter_ingestion_daily.py`, instead of saving our data down to the local PostgreSQL, we'll compress our data into Parquet and push to S3.

First, let's make sure we have the imports we need. You'll notice we removed `create_engine` and added `BytesIO` and `boto3`.

```
# At the top of ladot_parking_meter_ingestion_daily.py

from airflow.decorators import dag, task
import requests
import os
import pandas as pd
import boto3
from io import StringIO, BytesIO
```

Then, we update our `@task` and modify our `to_sql()` code block to to compress data into a Parquet and send off to S3:

```
@dag(
    ...
)
...
# In ladot_parking_meter_ingestion_daily.py
# Our previous sql-version Line 32 ~ Line 88 are replaced with:

    #1. set up variables to insert into request
    app_token = os.getenv("APP_TOKEN")
    s3_bucket = os.getenv("AWS_S3_BUCKET") # new
    
    current_date = datetime.now().strftime('%Y%m%d_%H%M%S') # new
    file_name = f"meter_occupancy/run_{current_date}.parquet" # new

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

    #3. convert to dataframe
    df = pd.read_csv(StringIO(meter_occupancy_response.text)) # changed, we don't need to iterate or chunk anymore

    #4. write to parquet in memory
    parquet_buffer = BytesIO() # new
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow", compression="snappy") # new

    #5. stream parquet file to S3
    s3_client = boto3.client("s3")  # new, the entire for loop is removed, and we send the data in one shot
    try:
        response =  s3_client.put_object(
            Bucket=s3_bucket,
            Key=file_name,
            Body=parquet_buffer.getvalue()
        )

        print(f"Successfully uploaded {file_name} to S3 bucket {s3_bucket}")
    except Exception as e:
        print(f"Error uploading {file_name} to S3 bucket {s3_bucket}: {e}")
        raise
...
ladot_dag = ladot_parking_meter_ingestion_daily()
```

With these new `@task` updates, we need to get our task updates up on Airflow now:

```
docker compose exec airflow-scheduler airflow dags reserialize
```

### Together with Terraform

We're moving step-by-step so it's easier to catch errors and breaks in the process. 

Now, we're ready to put another piece together: let's set up our [AWS infrastructure with Terraform](../terraform/README.md#in-summary).

In a different terminal tab:

```
cd terraform/
terraform init

# set your secret keys. replace the below values with your own
export AWS_ACCESS_KEY_ID="your_access_key_id_from_csv"
export AWS_SECRET_ACCESS_KEY="your_secret_access_key_from_csv"
export AWS_DEFAULT_REGION="us-west-1"

terraform plan
# check that the plan looks as it should

terraform apply
# Enter 'yes' after the prompt
```

Log in to your AWS dashboard to make sure your S3 bucket name created through Terraform matches the one you set in your `orchestration/.env` for key `AWS_S3_BUCKET`.

Back to your Airflow dashabord, manually run your DAG `ladot_parking_meter_ingestion_daily`. Click the `>` play icon, select "Single Run" and then click "Trigger".

Your DAG should show a green "success" checkmark ✅. Go into your AWS dashboard again, look for your S3 bucket, if you click into it, you should be able to see your `meter_occupancy` table

Within your S3 bucket should be the Parquet with a filename equivalent of `f"meter_occupancy/run_{current_date}.parquet"`.

If you see it there, it's a success!

### Extract & Load All Datasets

Great! Our DAG-to-AWS works well. Let's get it set up for the other 2 LADOT datasets we are going to work with.

We're going to create our main `ladot_parking_ingestion_daily.py` file to orchestrate all 3 datasets from LADOT using the Socrata Open Data API since these datasets are specifcally powered by Socrata.

```
ls
cd dags/
# Again, you need to be in your dags directory 
# e.g. /your-project-repo/orchestration/dags

touch ladot_parking_ingestion_daily.py
```

#### Using Socrata API

Here, we're going to refactor our ingestion function (from `pipeline/pipeline-sodapy.py` which worked for 1 dataset) into an `IngestionEngine` class with a method to handle the extract and load.

This way we can create 3 instances (and maybe more if we find another LADOT dataset useful to us) without rewriting large blocks of the same code. By using the Object Oriented approach in this specific case, we:
- have zero code duplication, making it much easier to maintain and
- make our code scalable so that adding new datasets into our pipeline only requires a few lines of code using configurations.

We don't need to change the imports from the above section. 
Let's highlight a few of the important parts of `ladot_parking_ingestion_daily.py`.

The reason we picked "LADOT Parking Meter Occupancy" to try out initially is because it only had ~4213 rows of data. Now that we're creating a Python class to also run ingestion for the 2 other datasets where one exceeds 25 million rows of data, we'll need to process in chunks.

1. Let's make sure we have the additional packages we'll need:

```
...
from sodapy import Socrata
from itertools import count
import json
```

Since `intertools` and `json` are built-in Python modules, we only need to add `sodapy` to our `requirements.txt`

```
...
# To connect to the Socrata Open Data API
sodapy
```

Because of this change, we'll need to rebuild Docker:

```
docker compose down
docker compose build --no-cache
docker compose up
```

You'll notice in this exercise, we had to move away from the `def chunked_iterable` function which was chunking _after_ we called the Socrata API for the entire dataset. This was creating `500 response` errors for our call to the 25 million rows dataset. We needed the make an adjustment to our code so that we're chunking _before_ calling the Socrata API.

```
# The class declaration
class IngestionEngine:
    def __init__(self, dataset_name: str, dataset_id: str):
        """We set our attribute definitions"""
        ...
        self.socrata_client = Socrata("data.lacity.org", self.app_token, timeout=120) # we need to increase the timeout in the case of extracting millions of rows.

    # This is our main data injection method and where we break out the data extraction into manageable chunks
    def extract_and_load_data(self, chunk_size: int = 20000):
        ...
        try:
            for i in count(start=0): # iterating through infinity (and conditionally breaking)
                offset = i * chunk_size

                chunked = self.socrata_client.get( # we are calling the Socrata API in chunks defined by chunk_size
                    self.dataset_id, 
                    limit=chunk_size,
                    offset=offset, # each time the loop runs, we make sure to start the extraction from where we left off previously
                    order=":id"
                )

                print(f"Fetched rows {offset} to {offset + chunk_size} in {self.dataset_name}")
                
                if not chunked or len(chunked) == 0:
                    break  # make sure we have a break so the loop doesn't run forever

                self._upload_chunk_to_s3(chunked, current_date, i) # we take the extraction and load it into S3

        except Exception as e:
            print(f"Error interruption while ingesting {self.dataset_name}: {e}")
            raise
        ...
```

Similar to what we did in the [standalone pipeline when extracting with the Socrata API](../pipeline/README.md#extraction-using-socrata-api), which did not integrate with Airflow nor AWS, we take our chunk, converted it to a DataFrame, and flattened dictionaries and lists before inserting into the db.

```
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
```

You can reference the entire `IngestionEngine` class in my `ladot_parking_ingestion_daily.py` file.

For each dataset, we create its own `@task` and an individual instance for each.

```
    @task(task_id="ingest_meter_occupancy_data")
    def extract_and_load_meter_occupancy_data():
        """
        For the LADOT Parking Meter Occupancy dataset
        """
        engine = IngestionEngine(
            dataset_name="meter_occupancy",
            dataset_id="e7h6-4a3e"
        )
        engine.extract_and_load_data()

    @task(task_id="ingest_parking_citations_data")
    def extract_and_load_parking_citations_data():
        ""
        For the Parking Citations dataset:
        ""
        engine = IngestionEngine(
            dataset_name="parking_citations",
            dataset_id="4f5p-udkv"
        )
        engine.extract_and_load_data()

    @task(task_id="ingest_parking_inventory_policies_data")
    def extract_and_load_parking_inventory_policies_data():
        """
        For the LADOT Metered Parking Inventory & Policies dataset
        """
        engine = IngestionEngine(
            dataset_name="parking_inventory_policies",
            dataset_id="s49e-q6j2"
        )
        engine.extract_and_load_data()

    # We need to instantiate each task
    run_meter_occupancy_data = extract_and_load_meter_occupancy_data()
    run_parking_citations_data = extract_and_load_parking_citations_data()
    run_parking_inventory_policies_data = extract_and_load_parking_inventory_policies_data()

    # this is Airflow's way of knowing to run these tasks one after the other
    run_meter_occupancy_data >> run_parking_inventory_policies_data >> run_parking_citations_data
```

Notice this line: `run_meter_occupancy_data >> run_parking_inventory_policies_data >> run_parking_citations_data`. We tell Airflow to run each task one after the other so the Socrata client doesn't reject our connection for running so many calls at the same time.

Now, we need to get our task updates up on Airflow now:

```
docker compose exec airflow-scheduler airflow dags reserialize
```

In your Airflow dashboard, when you go to Dags `localhost:8080/dags`, you should see `ladot_parking_ingestion_daily` listed. Click into that DAG and then click on "Tasks". You'll see our 3 tasks on that list.

Let's run them. Click on the `>` play button to trigger a manual run. "Single Run" > "Trigger".

A couple measures of success here:

1. In your Airflow dashboard, go to Dags `localhost:8080/dags`, click into this DAG we're working with: `ladot_parking_ingestion_daily`.
    - Click on "Runs"
    - Click into the most recent run "manual__2026-...+00:00"
    - Each task should be listed on this next screen. How many "✅ Successes" do you have? 
    - If just one or two, give it some time. The 25 million entry dataset will take a while
    - Once you see 3x "✅ Successes", you did it!

2. In your AWS S3 dashboard, go to "General purpose buckets"
    - Click on "ladot-meter-parking-de-project-aws-data-lake" (or the data lake name you created)
    - You should see 3x directories listed here, each of your dataset_name (e.g. 📁 meter_occupancy/)
    - Click into one who's task was "Success" in Airflow.
    - Do you see the parquet file there? (e.g. `run_20260611_201807_chunk_00000.parquet`)
    - If so, you're good to go and ready for AWS Glue!

### AWS Glue Crawler

1. Import `GlueCrawlerOperator` to your DAG.

```
# Top of ladot_parking_ingestion_daily.py
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
```

## Back to main

Excellent, you can [continue back at the main project](../README.md).