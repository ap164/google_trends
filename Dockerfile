# Base Apache Airflow image (latest version)
FROM apache/airflow:2.8.1

# Switch back to airflow user
USER airflow

# Copy requirements.txt and install Python dependencies in one layer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Expose port: 8080 (Airflow)
EXPOSE 8080
