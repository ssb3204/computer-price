FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    beautifulsoup4>=4.12 \
    requests>=2.31 \
    confluent-kafka>=2.3 \
    sqlalchemy>=2.0 \
    psycopg2-binary>=2.9 \
    snowflake-connector-python>=3.6 \
    pydantic-settings>=2.1 \
    rapidfuzz>=3.5

COPY src/ src/

CMD ["python", "-m", "src.consumers.change_detector"]
