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

Success means you should be able to access `https://localhost:8080` using the airflow/airflow username and password we set in .env.