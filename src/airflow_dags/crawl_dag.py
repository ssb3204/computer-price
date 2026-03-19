"""Daily price crawl DAG - runs at 06:00 KST (21:00 UTC)."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.common.config import load_settings
from src.common.kafka_client import create_producer
from src.common.serialization import serialize


def _crawl_site(site_module: str, site_class: str) -> None:
    import importlib

    settings = load_settings()
    producer = create_producer(settings.kafka)

    module = importlib.import_module(site_module)
    crawler_cls = getattr(module, site_class)
    crawler = crawler_cls()
    prices = crawler.crawl()

    for price in prices:
        producer.produce(
            topic="raw-prices",
            key=f"{price.site}:{price.product_name}".encode("utf-8"),
            value=serialize(price),
        )
    producer.flush()


default_args = {
    "owner": "computer_price",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_price_crawl",
    default_args=default_args,
    description="크롤링: 다나와, 컴퓨존, 견적왕 일일 가격 수집",
    schedule_interval="0 21 * * *",  # 06:00 KST = 21:00 UTC
    start_date=datetime(2026, 3, 19),
    catchup=False,
    max_active_runs=1,
    tags=["crawl", "price"],
) as dag:

    crawl_danawa = PythonOperator(
        task_id="crawl_danawa",
        python_callable=_crawl_site,
        op_args=["src.crawlers.danawa", "DanawaCrawler"],
    )

    crawl_compuzone = PythonOperator(
        task_id="crawl_compuzone",
        python_callable=_crawl_site,
        op_args=["src.crawlers.compuzone", "CompuzoneCrawler"],
    )

    crawl_pc_estimate = PythonOperator(
        task_id="crawl_pc_estimate",
        python_callable=_crawl_site,
        op_args=["src.crawlers.pc_estimate", "PCEstimateCrawler"],
    )

    [crawl_danawa, crawl_compuzone, crawl_pc_estimate]
