FROM astrocrpublic.azurecr.io/runtime:3.3-12

LABEL org.opencontainers.image.title="Daily Currency Exchange ETL" \
      org.opencontainers.image.description="Airflow ETL from Frankfurter to local MongoDB" \
      org.opencontainers.image.source="https://github.com/saadzafar0/airflow-etl" \
      io.assignment.networking="MongoDB on private currency-db Docker network" \
      io.assignment.storage="MongoDB data persisted in mongodb-data named volume"
