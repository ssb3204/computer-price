FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir \
    beautifulsoup4>=4.12 \
    requests>=2.31 \
    confluent-kafka>=2.3 \
    sqlalchemy>=2.0 \
    psycopg2-binary>=2.9 \
    pydantic-settings>=2.1 \
    rapidfuzz>=3.5

COPY src/ /opt/airflow/src/
