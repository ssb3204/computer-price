FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    dash>=2.14 \
    dash-bootstrap-components>=1.5 \
    plotly>=5.18 \
    pandas>=2.1 \
    sqlalchemy>=2.0 \
    psycopg2-binary>=2.9 \
    snowflake-connector-python>=3.6 \
    pydantic-settings>=2.1

COPY src/ src/

EXPOSE 8050

CMD ["python", "-m", "src.dashboard.app"]
