# Use the latest Airflow image as the base image
FROM apache/airflow:3.2.2

# Copy your requirements file into place
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt