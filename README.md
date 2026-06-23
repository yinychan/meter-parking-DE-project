# LA Parking Intelligence Pipeline
This is a data engineering pipeline that analyzes LADOT meter parking occupancy, citations, and inventory from LADOT Open Data. It reveals where open metered parking and which spaces have a higher likelihood of enforcement.

I provide the step-by-step process of building an end-to-end data pipeline, starting from configuring an efficient workflow. The full implementation stack includes AWS Glue, AWS S3, Apache Airflow 3.2.x, Terraform, Socrata API, Python, SQL, Docker, Codespaces, VS Code. Instructions are written for macOS. 

# Contents 

- [Workspace Setup](#workspace-setup)
  - [Github Codespaces](#github-codespaces)
  - [VS Code](#vs-code)
  - [Docker, Python, uv](#docker-python-uv)
- [Project Setup](#project-setup)
  - [Project files and dockerization](#project-files-and-dockerization)
  - [Environment Variables](#environment-variables)
  - [PostgreSQL in Docker](#postgresql-in-docker)
- [Data Ingestion](#data-ingestion) (This has its own README)
- [Docker Compose](#docker-compose) 
  - [Create](#create)
  - [Run](#run)
  - [Check](#check)
  - [Clean up](#clean-up)
- [Setting up AWS and Terraform](#setting-up-aws-and-terraform) (This als goes to its own README)
- [Workflow Orchestration with Apache Airflow 3.2.x](#workflow-orchestration-with-apache-airflow-32) (This goes to its own README)
- [Data Warehouse on Snowflake](#data-warehouse-on-snowflake)
- [Coming up](#coming-up) (All my plans for demonstrating this pipeline)


## Workspace Setup

### Github Codespaces
1. Create a new Github repository and initialize with Readme and python .gitignore.
2. Clone the repository into a Codespace 
    - Click on "<> Code"
    - Click on "Create codespace on main"
    - This should launch the browser-based workspace

We use Github Codespaces to remove setup friction and initialize the project from a standardized cloud setup. Codespaces comes with a preconfigured development environment without us having to configure it on our local desktop. The workflow here will launch a codespace, but we will use a desktop IDE (VS Code).

### VS Code
The next set of instructions assumes you have VS Code already installed on your local machine.

1. From the browser-based codespace, 
    - Click on the hamburger menu icon
    - Click on "Open VS Code Desktop"
2. Open the terminal:
    - Go to "Terminal"
    - Click "New Terminal"

We can run this codespace on the desktop using Visual Studio Code to avoid lag (as opposed to running it in-browser) and for a more productive workflow.

### Docker, Python, uv
We use Docker to make our program portable.
1. Docker is pre-installed by default, but let's check. 
And here's a [Docker CLI cheatsheet](https://docs.docker.com/get-started/docker_cheatsheet.pdf)
```
docker --version
```

2. Install python onto your codespace and check the python version
```
apt update && apt install python3
python3 -V
```
3. I prefer to use `uv` as the virtual environment for package management. I don't want to install packages globally on my system.
```
pip install uv
```

## Project Setup

### Project files and dockerization
1. Create your pipeline directory
```
mkdir pipeline
cd pipeline
touch pipeline.py
```
You can leave `pipeline.py` empty for now, or check that it works.
```
# in pipeline.py
import sys

say_what = sys.argv[1]
print(f"Repeat after me: {say_what}")
```

In your terminal, check that it works 
```
uv run python pipeline.py "Let's get this party started!"
```

2. Initialize a python project and check versions
```
uv init --python=3.14
uv run which python
uv run python -V
```
3. Let's setup a `Dockerfile` to create the Docker image
```
# Start an image with python on it
FROM python:3.14.4-slim

# Use uv in Docker 
# (docs: https://docs.astral.sh/uv/guides/integration/docker/)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Set working directory within image
WORKDIR /app

# Activate the project virtual environment to install packages 
# (https://docs.astral.sh/uv/guides/integration/docker/#using-the-environment)
ENV PATH="/app/.venv/bin:$PATH"

# Copy dependency files
COPY pyproject.toml .python-version uv.lock ./

# Install dependencies from lock file
RUN uv sync --locked

# Copy application code 
COPY pipeline.py pipeline.py

# Set entry point
ENTRYPOINT ["python", "pipeline.py"]
```

### Environment Variables
1. We will use a `.env` in developement
```
touch .env
```
2. Add to `.gitignore` so we make sure not to commit it from the go.
```
# in .gitignore
.env
.env.*
!.env.example
```
3. Create your own `.env` using `.env.example` as a template for all the key values we'll need for this project

### PostgreSQL in Docker
1. Create the directory for data persistence
```
mkdir la_meter_parking_postgres_data
```
2. Install PostgreSQL CLI as a development dependency
```
uv add --dev pgcli
```
3. Let's try it out in terminal. In one terminal window, run:
```
export $(xargs < .env)

docker run -it \
  -e POSTGRES_USER="$POSTGRES_USER" \
  -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  -e POSTGRES_DB="$POSTGRES_DB" \
  -v $(pwd)/$DATA_PERSISTENCE_DIR:/var/lib/postgresql \
  -p $POSTGRES_PORT:$POSTGRES_PORT \
  postgres:18
```

In another terminal window, run:
```
#use environment variables, or connect with your values in place
export $(xargs < .env)
uv run pgcli -h localhost -p $POSTGRES_PORT -u $POSTGRES_USER -d $POSTGRES_DB
```

Give your own password and then you should be in. Let's check
```
\dt
```
You should see a blank db schema

## Data Ingestion 
We test run a simple python script using `requests.post()` and `create_engine` to export, then ingest data from [Los Angeles Open Data](https://data.lacity.org/) into the PostgreSQL we created in Docker from the step above.

[Create a simple data ingestion script using python and PostgreSQL](pipeline/README.md)

## Docker Compose

We have an ingestion script that imports from one of the data endpoints we need, let's see how it would run inside a docker container.

### Create
1. Create a `docker-compose.yml` to simplify starting all services. We can run docker-compose to start up both the pipeline build and postgresql.

From your project root:
```
touch docker-compose.yml
```

In docker-compose.yml:
```
services:
  app:
    build: ./pipeline
    env_file:
      - .env.docker
    depends_on:
      postgres:
        condition: service_healthy

  postgres:
    image: postgres:18
    env_file:
      - .env.docker
    environment:
      POSTGRES_DB: la_meter_parking
    volumes:
      - la_meter_parking_postgres_data:/var/lib/postgresql
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "root", "-d", "la_meter_parking"]
      interval: 5s
      retries: 5

volumes:
  la_meter_parking_postgres_data:
```

Note that we're asking docker compose to read from `.env.docker`, so let's create that and use the same keys you had from `.env`:
```
touch .env.docker
```
And then set your values.

Because we've already added it to `.gitignore` at the beginning of the tutorial, this `.env.docker` should already be ignored.

### Run
2. Let's run it now
```
docker compose up --build
```

If all is well and good, your terminal should start and end with this:
```
[+] Building 1.3s (16/16) FINISHED                                                                                                                                               
=> [internal] load local bake definitions  
...
app-1       | Inserted: 100
app-1       | Inserted: 39
app-1 exited with code 0
```

### Check
3. Check that LA City's data is in our database. In a separate terminal, 
```
docker compose ps
```

Take the `NAME` that gets output
```
docker exec -it the_name_that_was_output psql -U same_as_POSTGRES_USER -d same_as_POSTGRES_DB
```

You should be in and can use `psql` to check on your newly created table from the `docker compose build`
```
\dt

SELECT * FROM meter_occupancy LIMIT 10;
```

An output like this means it works:
```
 index |   SpaceID    |      EventTime_UTC      | OccupancyState 
-------+--------------+-------------------------+----------------
     0 | WP150        | 2026 Jun 04 05:52:15 PM | OCCUPIED
     1 | CB588        | 2026 Jun 04 04:31:17 PM | OCCUPIED
     2 | C1087        | 2026 Jun 04 07:54:01 PM | OCCUPIED
     3 | WP69         | 2026 Jun 04 07:12:46 PM | VACANT
     4 | WP67         | 2026 Jun 04 07:30:50 PM | OCCUPIED
     5 | C291         | 2026 Jun 04 08:01:42 PM | OCCUPIED
     6 | V167         | 2026 Jun 04 03:37:15 PM | OCCUPIED
     7 | HO281B       | 2026 Jun 04 06:01:53 PM | OCCUPIED
     8 | HO879A-const | 2026 Jun 04 06:17:45 PM | UNKNOWN
     9 | SV34         | 2026 Jun 04 07:08:42 PM | OCCUPIED
(10 rows)
```

### Clean up
Delete anything you don't need
```
# Containers
docker ps -a
docker rm <container_id>

# Images
docker images
docker rmi <image>
docker rmi $(docker images -a -q) # deletes all images. add -f to force

# Volumes
docker volume ls
docker volume rm <volume_name>
docker volume prune -af

# Networks
docker network ls
docker network rm <network_id>
docker network prune
```

## Setting up AWS and Terraform
This section is in the /terraform file directory. It has everything we need to create an AWS S3 bucket and AWS Glue dataset through Terraform. We use Terraform variables to separate our settings from our main code.

[Getting Started with AWS & Terraform](terraform/README.md)

## Workflow Orchestration with Apache Airflow 3.2
Here, we go through the entire data workflow starting from getting up and running with Airflow in Docker, writing Airflow tasks with DAGs, data ingestion with the Socrata API, chunking our data, shipping our data off into AWS S3 (Data Lake), and creating an AWS Glue Crawler to get our Parquet data into our AWS Glue Data Catalog database. After which, we can move into actually doing something with our data in Analytics Engineering.

This is a pretty extensive and meaty section, so I will link to each part of this exercise:

- [Airflow & DAGs](orchestration/README.md#airflow--dags)
    - [Getting Started](orchestration/README.md#getting-started)
    - [Setting up Airflow](orchestration/README.md#setting-up-airflow)
    - [Airflow in Docker](orchestration/README.md#airflow-in-docker)
    - [Docker Compose for Airflow](orchestration/README.md#docker-compose-for-airflow)
    - [Build Docker Image with Airflow](orchestration/README.md#build-docker-image-with-airflow)
    - [DAGs](orchestration/README.md#dags)
    - [Running your DAG](orchestration/README.md#running-your-dag)

- [Cloud Data Lake with AWS](orchestration/README.md#cloud-data-lake-with-aws)
    - [Setting Up](orchestration/README.md#setting-up)
    - [Modify DAG for AWS S3 Ingestion](orchestration/README.md#modify-dag-for-aws-s3-ingestion)
    - [Together with Terraform](orchestration/README.md#together-with-terraform)
    - [Extract & Load All Datasets](orchestration/README.md#extract--load-all-datasets)
        - [Using Socrata API](orchestration/README.md#using-socrata-api)
    - [AWS Glue Crawler](orchestration/README.md#aws-glue-crawler)


## Data Warehouse on Snowflake
We configure our data warehouse infrastructure with Snowflake, staged to connect directly with our AWS S3 data lake. Snowflake allows us to decouple compute from storage and scale our query processing without the cost of increasing storage or leaving servers constantly running.
 
- [Concepts & Definitions](/warehouse/README.md#concepts--definitions)
    - [Database Systems](/warehouse/README.md#database-systems)
        - [Online Transactional Processing (OLTP)](/warehouse/README.md#online-transactional-processing-oltp)
        - [Online Analytical Processing (OLAP)](/warehouse/README.md#online-analytical-processing-olap)
    - [Staging Tables](/warehouse/README.md#staging-tables)
    - [Data Normalization](/warehouse/README.md#data-normalization)
    - [Data Denormalization](/warehouse/README.md#data-denormalization)
        - [Fact Tables](/warehouse/README.md#fact-tables)
        - [Dimension Tables](/warehouse/README.md#dimension-tables)
        - [Star Schema](/warehouse/README.md#star-schema)
- [Snowflake Architecture](/warehouse/README.md#snowflake-architecture)
    - [Secure Cloud Authentication](/warehouse/README.md#secure-cloud-authentication)

## Next up: Batch Processing and Analytics Engineering
We will implement batch processing and analytics engineering with Apache Spark. Because of the raw public data we pulled from LADOT's API via Socrata, our data was extracted into Parquet files as standard text strings. We will run a local environment of Spark to read the Parquet files and re-type the fields into their original datatypes of numbers and dates.

## Coming up
This project is in progress. Here is what to expect in the coming days and weeks:
- ~~Infrastructure-as-Code using Terraform~~
- ~~Workflow orchestration with Apache Airflow~~
- ~~Write a DAG with Airflow~~
- ~~Data warehouse on Snowflake~~
- Analytics engineering with Apache Spark and PySpark
- Batch processing with Apache Spark and PySpark
- Modeling with Star Schema
- Data Analytics & Machine Learning