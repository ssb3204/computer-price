FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*
USER airflow

# Airflow 2.8은 SQLAlchemy <2.0 필요 → pyproject.toml 대신 직접 설치
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
        beautifulsoup4>=4.12 \
        requests>=2.31 \
        confluent-kafka>=2.3 \
        snowflake-connector-python>=3.6 \
        pydantic-settings>=2.1 \
        rapidfuzz>=3.5

COPY src/ /opt/airflow/src/
