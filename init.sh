#!/bin/bash
# filepath: ./init.sh

set -e


if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# Build Docker images without using cache
echo "Building images without cache..."
docker compose build --no-cache

# Start all containers in detached mode
echo "Starting containers..."
docker compose up -d
# Wait for Airflow Webserver to be ready (check health endpoint)
echo "Waiting for Airflow Webserver to start..."
until docker compose exec airflow-webserver curl -sf http://localhost:8080/health; do
  printf '.'
  sleep 5
done

# Create admin user in Airflow
echo "Creating admin user in Airflow..."
docker compose exec airflow-webserver airflow users create \
  --username admin \
  --password admin \
  --firstname "${AIRFLOW_ADMIN_FIRSTNAME}" \
  --lastname "${AIRFLOW_ADMIN_LASTNAME}" \
  --role Admin \
  --email "${AIRFLOW_ADMIN_EMAIL}"

# Run tests inside the webserver container
echo "Running tests inside airflow-webserver..."
docker compose exec airflow-webserver pytest /opt/airflow/tests

echo "Done!"
echo "Done!"
# chmod +x init.sh
# ./init.sh
# docker compose down