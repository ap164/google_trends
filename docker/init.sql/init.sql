CREATE TABLE IF NOT EXISTS dane_w_czasie (
    data TIMESTAMP NOT NULL,
    slowo VARCHAR(255) NOT NULL,
    wynik FLOAT NOT NULL,
    PRIMARY KEY (data, slowo)
);

-- init.sql
CREATE TABLE IF NOT EXISTS cele_kampanii (
    id_celu SERIAL PRIMARY KEY,
    nazwa_celu VARCHAR(255) NOT NULL
);
