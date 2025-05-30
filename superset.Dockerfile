FROM apache/superset:latest

USER root

# Zainstaluj sterownik PostgreSQL
RUN pip install psycopg2-binary

USER superset
