# Użyj oficjalnego obrazu Apache Airflow jako bazowego
FROM apache/airflow:2.6.3

# Przełącz na użytkownika root, aby zainstalować pakiety systemowe
USER root

# Zainstaluj zależności systemowe wymagane przez confluent-kafka oraz supervisord
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    librdkafka-dev \
    supervisor \
    netcat \
    && rm -rf /var/lib/apt/lists/*

# Przełącz z powrotem na użytkownika airflow
USER airflow

# Ustaw katalog roboczy
WORKDIR /app

# Skopiuj plik requirements.txt do obrazu
COPY requirements.txt .

# Zainstaluj pakiety Python z requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Zainstaluj dodatkowe pakiety Python, w tym Flask, psycopg2-binary, biblioteki związane z Kafka
RUN pip install --no-cache-dir Flask psycopg2-binary confluent-kafka pandas requests imapclient

# Skopiuj pliki aplikacji Flask
COPY webapp/ /app/

# Skopiuj skrypty producenta i konsumenta Kafka
COPY kafka_scripts/ /app/kafka_scripts/

# Skopiuj plik konfiguracyjny supervisord
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Ustaw zmienną środowiskową PYTHONPATH, aby uwzględnić katalog /app
ENV PYTHONPATH="${PYTHONPATH}:/app"

# Otwórz potrzebne porty
EXPOSE 8080 5001

# Ustaw domyślną komendę do uruchomienia supervisord
CMD ["/usr/bin/supervisord"]
