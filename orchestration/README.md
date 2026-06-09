## Date Workflow Orchestration
We will be setting up Apache Airflow using Docker and working with AWS. Airflow acts as a "centralized control room" for our data engineering pipelines. Instead of running Python extraction scripts on a manual timer or using a cron job, Airflow orchestrates the entire data workflow.

### Getting Started
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
# AWS Provider 
apache-airflow-providers-amazon
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
```

For your convenience, here's the link to [Generate an Access Key Pair](../terraform/README.md) in the Terraform exercise where you generated an `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

To generate your own Fernet key, run this in your terminal:

```
uv run --with cryptography python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
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

# You need to be in your dags directory e.g. /your-project-repo/orchestration/dags
touch ladot_parking_ingestion_daily.py
```

1. Import Airflow directo decorators (`@dag` and `@task`) along with calendar tracking tools

```
# In ladot_parking_ingestion_daily.py

from datetime import datetime, timedelta
from airflow.decorators import dag, task
```

2. Decorate your main wrapper function to define pipeline settings. I've gone ahead and commented on further descriptions of certain options we need.

```
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
```

3. Write out your main pipeline ingestion logic. To start, we copy the raw Python from `pipeline.py` in an [earlier pipeline exercise](../pipeline/README.md) and paste into our new piepline function

```
@dag(
    ...
)
def ladot_parking_ingestion_daily():
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

Import our pipeline tools at the top of the file

```
# In ladot_parking_ingestion_daily.py

import requests
import os
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from tqdm.auto import tqdm
```

Note that we'll need to default our db host to `postgres` since we'll be running in Docker. 

```
# Line ~63 in ladot_parking_ingestion_daily.py

psql_host = os.getenv("POSTGRES_HOST", "postgres")
```

Make sure you add your APP_TOKEN from the pipeline exercise, or [generate their API Token](../pipeline/README.md) to enter the `APP_TOKEN` value in your `.env` for this exercise.

```
# In /project/orchestration/.env, switch out the value for your actual token

APP_TOKEN=app_token_from_https://data.lacity.org/profile/edit/developer_settings
```

4. Let's give it a go. Instantiate the execution and call the main entrypoint function

```
    # Instantiate the task(s)
    run_meter_occupancy_data = extract_and_load_meter_occupancy_data()

# Instantiate DAG
ladot_dag = ladot_parking_ingestion_daily()
```

5. Since we've updated the `.env` file, we'll ned to bring Docker down then back up again

```
docker compose down -v
docker compose build --no-cache
docker compose up airflow-init
docker compose up
```

6. Helpful Airflow commands:

```
# Force an immediate rescan (force the running scheduler to process newly fixed code in your dag file)
docker compose exec airflow-scheduler airflow dags reserialize

# List all the dags
docker compose exec airflow-scheduler airflow dags list
```

#### DAG in Airflow Dashboard

Success here means you can see your DAG within your localhost:8080 Airflow Dashboard