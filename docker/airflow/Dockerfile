FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*
USER airflow

COPY pyproject.toml /opt/airflow/
RUN pip install --no-cache-dir /opt/airflow/

COPY src/ /opt/airflow/src/
