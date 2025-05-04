import os
import logging
import psycopg2
import smtplib
from pytrends.request import TrendReq
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from config import (
    POSTGRES_HOST,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    SENDER_EMAIL,
    SENDER_EMAIL_PASSWORD,
    RECIPIENT_EMAIL,
    PYTRENDS_TIMEOUT,
    LOG_LEVEL,
    PYTRENDS_CONFIGS
)

# Konfiguracja logowania
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

# Funkcja do wysyłania e-maili
def send_email(subject, message, recipient_email):
    """Wysyła e-mail z podanym tematem i treścią."""
    if not SENDER_EMAIL or not SENDER_EMAIL_PASSWORD:
        logger.error("Brak danych do wysłania e-maila. Sprawdź konfigurację.")
        return

    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.attach(MIMEText(message, 'plain'))

    try:
        with smtplib.SMTP('mail.gmx.com', 587) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_EMAIL_PASSWORD)
            server.sendmail(SENDER_EMAIL, recipient_email, msg.as_string())
        logger.info(f"E-mail został wysłany do {recipient_email} z tematem: {subject}")
    except Exception as e:
        logger.error(f"Nie udało się wysłać e-maila: {e}")

# Funkcja do połączenia z PostgreSQL
def connect_to_postgres():
    """Łączy się z bazą danych PostgreSQL."""
    try:
        connection = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        logger.info("Połączono z bazą danych PostgreSQL.")
        return connection
    except Exception as e:
        logger.error(f"Błąd połączenia z bazą danych: {e}")
        return None

# Funkcja do pobierania danych z PyTrends
def fetch_pytrends_data(pytrends, keyword, timeframe, category, geo, gprop):
    """Pobiera dane z PyTrends dla danego słowa kluczowego."""
    try:
        pytrends.build_payload(
            [keyword],
            cat=category,
            timeframe=timeframe,
            geo=geo,
            gprop=gprop
        )
        interest_over_time = pytrends.interest_over_time()
        
        # Logowanie danych zwróconych przez PyTrends
        if interest_over_time is not None and not interest_over_time.empty:
            logger.info(f"Dane zwrócone przez PyTrends dla słowa '{keyword}':\n{interest_over_time.head()}")
        else:
            logger.warning(f"Brak danych zwróconych przez PyTrends dla słowa '{keyword}'.")
        
        return interest_over_time
    except Exception as e:
        logger.error(f"Błąd podczas pobierania danych z PyTrends dla słowa '{keyword}': {e}")
        return None

# Funkcja do przetwarzania i zapisywania danych w PostgreSQL
def process_and_store_data(cursor, connection, interest_hourly, keyword):
    """Przetwarza dane i zapisuje je do bazy danych."""
    try:
        for index, row in interest_hourly.iterrows():
            data = index
            wynik = row[keyword]
            insert_query = """
            INSERT INTO dane_w_czasie (data, slowo, wynik) 
            VALUES (%s, %s, %s)
            """
            record_to_insert = (data, keyword, wynik)
            cursor.execute(insert_query, record_to_insert)
            connection.commit()
        logger.info(f"Dane dla słowa '{keyword}' zostały zapisane do tabeli 'dane_w_czasie'.")
    except Exception as e:
        logger.error(f"Błąd podczas zapisywania danych dla słowa '{keyword}': {e}")

# Główna funkcja do pobierania i zapisywania danych
def fetch_and_store_pytrends(configs):
    """Pobiera dane z PyTrends i zapisuje je do PostgreSQL."""
    connection = connect_to_postgres()
    if not connection:
        logger.error("Nie udało się nawiązać połączenia z bazą danych. Przerywam działanie.")
        return

    pytrends = TrendReq(tz=360, timeout=PYTRENDS_TIMEOUT)

    try:
        with connection:
            cursor = connection.cursor()
            for config in configs:
                keywords = config["keywords"]
                timeframe = config["timeframe"]
                category = config["category"]
                geo = config["geo"]
                gprop = config["gprop"]

                for keyword in keywords:
                    interest_over_time = fetch_pytrends_data(pytrends, keyword, timeframe, category, geo, gprop)
                    if interest_over_time is not None and not interest_over_time.empty:
                        process_and_store_data(cursor, connection, interest_over_time, keyword)
                    else:
#                        subject = f"Brak danych o zainteresowaniu dla słowa '{keyword}'"
#                        message = f"Brak danych o zainteresowaniu w czasie dla słowa '{keyword}'"
#                        send_email(subject, message, RECIPIENT_EMAIL)
                        logger.warning(f"Brak danych o zainteresowaniu w czasie dla słowa '{keyword}'.")
    except Exception as e:
        subject = "Błąd podczas pobierania danych o zainteresowaniu w czasie"
        message = f"Błąd: {e}"
        send_email(subject, message, RECIPIENT_EMAIL)
        logger.error(f"Błąd podczas pobierania danych: {e}")
    finally:
        connection.close()
        logger.info("Połączenie z bazą danych zostało zamknięte.")
