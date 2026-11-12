"""Data-contract and extraction failure tests; no network calls are needed."""

from copy import deepcopy
from unittest.mock import Mock, patch

import pytest
import requests

from include.currency_etl import currency_config, extract_rates, transform_rates


@pytest.fixture
def payload():
    return {
        "retrieved_at": "2026-11-12T07:15:00+00:00",
        "rates": [
            {"date": "2026-11-11", "base": "EUR", "quote": "USD", "rate": 1.15},
            {"date": "2026-11-11", "base": "EUR", "quote": "GBP", "rate": 0.83},
            {"date": "2026-11-11", "base": "EUR", "quote": "JPY", "rate": 177.0},
        ],
    }


def test_currency_configuration_is_normalized():
    assert currency_config(" eur ", "usd, gbp,jpy") == ("EUR", ["USD", "GBP", "JPY"])


@pytest.mark.parametrize("base,targets", [
    ("EU", "USD"), ("EUR", ""), ("EUR", "USD,"),
    ("EUR", "USD,USD"), ("EUR", "EUR"), ("EUR", "US1"),
])
def test_invalid_configuration_is_rejected(base, targets):
    with pytest.raises(ValueError):
        currency_config(base, targets)


def test_transformation_preserves_publication_date_and_timestamp(payload):
    records = transform_rates(payload, "EUR", ["USD", "GBP", "JPY"])
    assert len(records) == 3
    assert records[-1] == {
        "base_currency": "EUR", "target_currency": "USD", "exchange_rate": 1.15,
        "rate_date": "2026-11-11", "source": "Frankfurter API",
        "retrieved_at": "2026-11-12T07:15:00+00:00",
    }
    assert all(len(record) == 6 for record in records)


@pytest.mark.parametrize("rate", [0, -1, True, "1.15", None, float("nan"), float("inf")])
def test_invalid_rates_are_rejected(payload, rate):
    payload["rates"][0]["rate"] = rate
    with pytest.raises(ValueError):
        transform_rates(payload, "EUR", ["USD", "GBP", "JPY"])


@pytest.mark.parametrize("field,value", [
    ("base", "USD"), ("quote", "CAD"), ("date", "2026-13-01"),
    ("date", "2026-11-13"), ("date", None),
])
def test_malformed_api_rows_are_rejected(payload, field, value):
    payload["rates"][0][field] = value
    with pytest.raises(ValueError):
        transform_rates(payload, "EUR", ["USD", "GBP", "JPY"])


def test_duplicate_and_missing_targets_are_rejected(payload):
    missing = deepcopy(payload)
    missing["rates"].pop()
    payload["rates"].append(payload["rates"][0])
    for invalid in [missing, payload]:
        with pytest.raises(ValueError):
            transform_rates(invalid, "EUR", ["USD", "GBP", "JPY"])


@pytest.mark.parametrize("payload", [None, {}, {"rates": []}, {"rates": [], "retrieved_at": "2026-11-12T07:15:00"}])
def test_missing_xcom_or_timestamp_is_rejected(payload):
    with pytest.raises(ValueError):
        transform_rates(payload, "EUR", ["USD", "GBP", "JPY"])


def test_http_failure_is_propagated_for_airflow_retry():
    response = Mock()
    response.raise_for_status.side_effect = requests.HTTPError("503 unavailable")
    with patch("include.currency_etl.requests.get", return_value=response):
        with pytest.raises(requests.HTTPError):
            extract_rates("EUR", ["USD"])


@pytest.mark.parametrize("body", [[], {}, None])
def test_empty_or_wrong_shape_api_response_is_rejected(body):
    response = Mock()
    response.json.return_value = body
    with patch("include.currency_etl.requests.get", return_value=response):
        with pytest.raises(ValueError):
            extract_rates("EUR", ["USD"])


def test_extract_uses_v2_query_timeout_and_aware_timestamp(payload):
    response = Mock()
    response.json.return_value = payload["rates"]
    with patch("include.currency_etl.requests.get", return_value=response) as get:
        result = extract_rates("EUR", ["USD", "GBP", "JPY"])
    assert get.call_args.kwargs == {
        "params": {"base": "EUR", "quotes": "USD,GBP,JPY"}, "timeout": (10, 30),
    }
    assert result["rates"] == payload["rates"]
    assert result["retrieved_at"].endswith("+00:00")
