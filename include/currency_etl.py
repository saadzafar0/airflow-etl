"""HTTP extraction, strict transformation, and retry-safe MongoDB loading."""

from datetime import date, datetime, timezone
import math
import re

import requests
from pymongo import ASCENDING, MongoClient, UpdateOne

API_URL = "https://api.frankfurter.dev/v2/rates"
SOURCE = "Frankfurter API"
RAW_XCOM_KEY = "raw_exchange_rates"
TRANSFORMED_XCOM_KEY = "transformed_exchange_rates"
LOAD_XCOM_KEY = "mongo_load_summary"


def currency_config(base="EUR", targets="USD,GBP,JPY"):
    """Normalize configuration and reject invalid or duplicate currency codes."""
    base = base.strip().upper()
    quotes = [code.strip().upper() for code in targets.split(",")]
    if not re.fullmatch(r"[A-Z]{3}", base) or not quotes:
        raise ValueError("Base and targets must be three-letter currency codes")
    if any(not re.fullmatch(r"[A-Z]{3}", code) for code in quotes):
        raise ValueError("Targets must be comma-separated three-letter currency codes")
    if len(quotes) != len(set(quotes)) or base in quotes:
        raise ValueError("Targets must be unique and different from the base")
    return base, quotes


def extract_rates(base, targets):
    """Retrieve the latest published rates; Airflow handles transient retries."""
    response = requests.get(
        API_URL, params={"base": base, "quotes": ",".join(targets)}, timeout=(10, 30)
    )
    response.raise_for_status()
    rows = response.json()
    if not isinstance(rows, list) or not rows:
        raise ValueError("Frankfurter returned an empty or invalid rates array")
    return {
        "rates": rows,
        "retrieved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def transform_rates(payload, base, targets):
    """Validate complete API results and map them to the assignment's six fields."""
    if not isinstance(payload, dict) or not isinstance(payload.get("rates"), list):
        raise ValueError("Missing raw_exchange_rates XCom payload")
    retrieved_at = payload.get("retrieved_at")
    if not isinstance(retrieved_at, str):
        raise ValueError("Missing retrieval timestamp")
    timestamp = datetime.fromisoformat(retrieved_at)
    if timestamp.tzinfo is None:
        raise ValueError("Retrieval timestamp must include a timezone")
    records, seen = [], set()
    for row in payload["rates"]:
        if not isinstance(row, dict):
            raise ValueError("Each API rate must be an object")
        quote, rate = row.get("quote"), row.get("rate")
        if row.get("base") != base or quote not in targets or quote in seen:
            raise ValueError("Unexpected base, target, or duplicate currency in API response")
        if isinstance(rate, bool) or not isinstance(rate, (int, float)):
            raise ValueError("Exchange rate must be numeric")
        if not math.isfinite(rate) or rate <= 0:
            raise ValueError("Exchange rate must be finite and positive")
        rate_date = row.get("date")
        if not isinstance(rate_date, str) or date.fromisoformat(rate_date).isoformat() != rate_date:
            raise ValueError("Rate date must use YYYY-MM-DD")
        if rate_date > timestamp.date().isoformat():
            raise ValueError("Rate date cannot be later than retrieval date")
        records.append({
            "base_currency": base,
            "target_currency": quote,
            "exchange_rate": float(rate),
            "rate_date": rate_date,
            "source": SOURCE,
            "retrieved_at": retrieved_at,
        })
        seen.add(quote)
    if seen != set(targets):
        raise ValueError("API response is missing one or more requested target currencies")
    return sorted(records, key=lambda record: record["target_currency"])


def load_rates(records, uri, database="currency_exchange", collection="exchange_rates"):
    """Upsert by pair and publication date so retries cannot create duplicates."""
    if not isinstance(records, list) or not records:
        raise ValueError("Missing transformed_exchange_rates XCom records")
    with MongoClient(uri, serverSelectionTimeoutMS=10000) as client:
        client.admin.command("ping")
        target = client[database][collection]
        target.create_index(
            [("base_currency", ASCENDING), ("target_currency", ASCENDING), ("rate_date", ASCENDING)],
            unique=True, name="unique_currency_pair_date",
        )
        result = target.bulk_write([
            UpdateOne(
                {key: record[key] for key in ("base_currency", "target_currency", "rate_date")},
                {"$set": record}, upsert=True,
            )
            for record in records
        ], ordered=False)
        return {
            "database": database, "collection": collection,
            "processed": len(records), "inserted": result.upserted_count,
            "matched": result.matched_count, "modified": result.modified_count,
            "total_documents": target.count_documents({}),
        }
