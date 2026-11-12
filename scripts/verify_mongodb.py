"""Read real persisted records and indexes from the local Docker MongoDB."""

import json
import os

from pymongo import MongoClient


def main():
    with MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=10000) as client:
        collection = client[os.getenv("MONGO_DATABASE", "currency_exchange")][
            os.getenv("MONGO_COLLECTION", "exchange_rates")
        ]
        records = list(collection.find({}, {"_id": 0}).sort("target_currency", 1))
        indexes = collection.index_information()
        print(json.dumps({"document_count": len(records), "records": records, "indexes": indexes}, indent=2))
        assert records, "No exchange-rate records were loaded"
        assert indexes["unique_currency_pair_date"]["unique"] is True
        assert all(set(record) == {
            "base_currency", "target_currency", "exchange_rate", "rate_date", "source", "retrieved_at",
        } for record in records)


if __name__ == "__main__":
    main()
