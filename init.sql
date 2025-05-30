CREATE SCHEMA IF NOT EXISTS AIRFLOW_DB;
CREATE SCHEMA IF NOT EXISTS AIRFLOW_RAPORTS;

CREATE TABLE IF NOT EXISTS AIRFLOW_DB.interest_over_time (
    date TIMESTAMP NOT NULL,
    keyword VARCHAR(255) NOT NULL,
    value FLOAT NOT NULL,
    channel VARCHAR(255) NOT NULL,
    schedule_interval VARCHAR(255) NOT NULL,
    CONSTRAINT unique_interest_over_time UNIQUE (date, keyword, channel, schedule_interval),
    geo_search VARCHAR(255);
);

CREATE TABLE IF NOT EXISTS AIRFLOW_DB.interest_by_region (
    region VARCHAR(255) NOT NULL,
    keyword VARCHAR(255) NOT NULL,
    value SMALLINT NOT NULL,
    schedule_interval VARCHAR(255) NOT NULL,
    load_date DATE NOT NULL,
    CONSTRAINT unique_interest_by_region UNIQUE (region, keyword, schedule_interval, load_date),
    geo_search VARCHAR(255);
);