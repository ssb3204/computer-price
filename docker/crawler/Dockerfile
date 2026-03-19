FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    beautifulsoup4>=4.12 \
    requests>=2.31 \
    confluent-kafka>=2.3 \
    pydantic-settings>=2.1 \
    rapidfuzz>=3.5

COPY src/ src/

CMD ["python", "-m", "src.crawlers.danawa"]
