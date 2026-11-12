"""Daily currency exchange ETL with three tasks and explicitly named XComs."""

from datetime import timedelta
import logging
import os

import pendulum
from airflow.sdk import DAG, get_current_context
from airflow.providers.standard.operators.python import PythonOperator

from include.currency_etl import (
    RAW_XCOM_KEY, TRANSFORMED_XCOM_KEY, LOAD_XCOM_KEY,
    currency_config, extract_rates, transform_rates, load_rates,
)

log = logging.getLogger(__name__)


def extract_currency_exchange_rates():
    base, targets = currency_config(
        os.getenv("BASE_CURRENCY", "EUR"), os.getenv("TARGET_CURRENCIES", "USD,GBP,JPY")
    )
    payload = extract_rates(base, targets)
    get_current_context()["ti"].xcom_push(key=RAW_XCOM_KEY, value=payload)
    log.info("Extracted %d exchange rates", len(payload["rates"]))


def transform_exchange_rate_data():
    ti = get_current_context()["ti"]
    payload = ti.xcom_pull(task_ids="extract_currency_exchange_rates", key=RAW_XCOM_KEY)
    base, targets = currency_config(
        os.getenv("BASE_CURRENCY", "EUR"), os.getenv("TARGET_CURRENCIES", "USD,GBP,JPY")
    )
    records = transform_rates(payload, base, targets)
    ti.xcom_push(key=TRANSFORMED_XCOM_KEY, value=records)
    log.info("Transformed records: %s", records)


def load_data_into_mongodb():
    ti = get_current_context()["ti"]
    records = ti.xcom_pull(task_ids="transform_exchange_rate_data", key=TRANSFORMED_XCOM_KEY)
    summary = load_rates(
        records, os.environ["MONGO_URI"],
        os.getenv("MONGO_DATABASE", "currency_exchange"),
        os.getenv("MONGO_COLLECTION", "exchange_rates"),
    )
    ti.xcom_push(key=LOAD_XCOM_KEY, value=summary)
    log.info("MongoDB load summary: %s", summary)


with DAG(
    dag_id="currency_exchange_etl",
    description="Daily Frankfurter currency rates to local MongoDB",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Karachi"),
    schedule="15 7 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "saadzafar0", "retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["etl", "currency", "mongodb", "frankfurter"],
    doc_md="""
    Runs daily at **07:15 Asia/Karachi (02:15 UTC)**. Uses the latest published
    rate date from Frankfurter, including on weekends and holidays.
    Named XComs: `raw_exchange_rates` -> `transformed_exchange_rates` -> `mongo_load_summary`.
    MongoDB uses a unique pair/date index and upserts for retry-safe loading.
    """,
) as dag:
    extract = PythonOperator(
        task_id="extract_currency_exchange_rates",
        python_callable=extract_currency_exchange_rates, do_xcom_push=False,
    )
    transform = PythonOperator(
        task_id="transform_exchange_rate_data",
        python_callable=transform_exchange_rate_data, do_xcom_push=False,
    )
    load = PythonOperator(
        task_id="load_data_into_mongodb",
        python_callable=load_data_into_mongodb, do_xcom_push=False,
    )
    extract >> transform >> load
