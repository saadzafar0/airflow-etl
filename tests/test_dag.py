"""Run inside the Astro Runtime to check real Airflow DAG parsing."""

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag


def test_dag_imports_and_has_exactly_three_sequential_tasks():
    bag = DagBag(dag_folder="dags")
    assert not bag.import_errors, bag.import_errors
    dag = bag.dags.get("currency_exchange_etl")
    assert dag is not None
    assert set(dag.task_ids) == {
        "extract_currency_exchange_rates", "transform_exchange_rate_data", "load_data_into_mongodb",
    }
    assert dag.get_task("extract_currency_exchange_rates").downstream_task_ids == {"transform_exchange_rate_data"}
    assert dag.get_task("transform_exchange_rate_data").downstream_task_ids == {"load_data_into_mongodb"}
    assert str(dag.timezone) == "Asia/Karachi"
    assert not dag.catchup
    assert dag.max_active_runs == 1
