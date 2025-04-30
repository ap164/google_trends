# ✅ Bazowy obraz Apache Airflow (nowsza wersja)
FROM apache/airflow:2.8.1

# ✅ Przełącz na użytkownika root, aby instalować systemowe zależności
USER root

# ✅ Instalacja zależności systemowych dla Kafka i supervisord w jednej warstwie
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    librdkafka-dev \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# ✅ Przełącz z powrotem na użytkownika airflow
USER airflow

# ✅ Ustaw katalog roboczy
WORKDIR /app

# ✅ Skopiuj plik requirements.txt i zainstaluj zależności Python w jednej warstwie
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ✅ Skopiuj aplikację Flask i skrypty Kafka producer/consumer w jednej warstwie
COPY webapp/ /app/
COPY kafka_scripts/ /app/kafka_scripts/

# ✅ Skopiuj konfigurację supervisorda
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# ✅ Ustaw PYTHONPATH, żeby `/app` był widoczny w Pythonie
ENV PYTHONPATH="${PYTHONPATH}:/app"

# ✅ Otwórz porty: 8080 (Airflow) i 5001 (webapp)
EXPOSE 8080 5001

# ✅ Uruchom supervisord jako domyślną komendę
CMD ["/usr/bin/supervisord"]
