#!/bin/bash

# Configure the credentials
DB_NAME="apex_db"
DB_USER="postgres"
DB_PASSWORD="amdari2025"
GCS_BUCKET="apexamdaribucket"

export PGPASSWORD="$DB_PASSWORD"

TABLES=("ghana_customers" "ghana_transaction" "kenya_customers" \
        "kenya_transaction" "nigeria_customers" "nigeria_transaction")

mkdir -p pg_exports

for TABLE in "${TABLES[@]}"
do
    echo "Exporting $TABLE..."

    # Export table to CSV and compress
    psql -U "$DB_USER" -d "$DB_NAME" -c "\copy $TABLE TO STDOUT WITH CSV HEADER" | gzip > "pg_exports/$TABLE.csv.gz"

    # Upload to GCS
    gsutil cp "pg_exports/$TABLE.csv.gz" "gs://$GCS_BUCKET/"

    echo "$TABLE.csv.gz uploaded to gs://$GCS_BUCKET/."
done

echo "All datasets processed."










