import os
import psycopg2
import smtplib
from pytrends.request import TrendReq
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()


def send_email(subject, message, recipient_email):
    sender_email = os.getenv("SENDER_EMAIL")  # Adres e-mail pobrany z pliku .env
    sender_password = os.getenv("SENDER_EMAIL_PASSWORD")  # Hasło pobrane z pliku .env

    if not sender_email or not sender_password:
        print("Brak danych do wysłania e-maila. Sprawdź konfigurację pliku .env.")
        return

    # Tworzenie wiadomości
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.attach(MIMEText(message, 'plain'))

    try:
        # Połączenie z serwerem SMTP GMX
        server = smtplib.SMTP('mail.gmx.com', 587)  # Użycie serwera SMTP GMX
        server.starttls()  # Inicjalizacja bezpiecznego połączenia TLS
        server.login(sender_email, sender_password)  # Logowanie na konto GMX
        text = msg.as_string()
        server.sendmail(sender_email, recipient_email, text)  # Wysyłanie wiadomości
        server.quit()  # Zakończenie połączenia
        # print("Email został wysłany.")
    except Exception as e:
        print(f"Nie udało się wysłać e-maila: {e}")

# Funkcja do pobierania danych xz Pytrends i zapisywania do PostgreSQL
def fetch_and_store_pytrends():
    # Połączenie z bazą danych PostgreSQL w Dockerze
    try:
        connection = psycopg2.connect(
            host="postgres",  # Nazwa usługi Docker dla PostgreSQL
            database="airflow",  # Nazwa bazy danych
            user="airflow",  # Użytkownik
            password="airflow"  # Hasło
        )
        cursor = connection.cursor()
        print("Połączono z bazą danych PostgreSQL")
    except Exception as e:
        print(f"Błąd połączenia z bazą danych: {e}")
        return

    # Email odbiorcy
    recipient_email = "prusa5100@gmail.com"
    
    try:
        # Inicjalizacja Pytrends
        pytrends = TrendReq(hl='pl-PL', tz=360, timeout=(10, 25))

        # Lista słów kluczowych
        kw_list = ["b2b"]  # Lista słów kluczowych

        for slowo in kw_list:
            # Ustawienia: Polska, dane z ostatnich 24 godzin (Google search)
            pytrends.build_payload([slowo], cat=0, timeframe='now 1-d', geo='PL-MZ', gprop='youtube')

            interest_over_time = pytrends.interest_over_time()
            if not interest_over_time.empty:
                print(f"\nZainteresowanie w czasie dla słowa '{slowo}' (dane minutowe z ostatnich 24 godzin):")
                print(interest_over_time.head())  # Wyświetla pierwsze 5 wierszy

                # Resample - grupowanie minutowych danych na godziny i liczenie średniej
                interest_hourly = interest_over_time.resample('H').mean().round(2)

                # Iteracja po wierszach danych przetworzonych godzinowo i zapisanie ich do bazy danych
                for index, row in interest_hourly.iterrows():
                    data = index  # Kolumna z resamplowaną datą (godzina)
                    wynik = row[slowo]  # Kolumna z wynikiem dla danego słowa

                    # SQL do wstawienia danych do tabeli
                    insert_query = """
                    INSERT INTO dane_w_czasie (data, slowo, wynik) 
                    VALUES (%s, %s, %s)
                    """
                    record_to_insert = (data, slowo, wynik)

                    # Wykonanie zapytania
                    cursor.execute(insert_query, record_to_insert)
                    connection.commit()

                print(f"Dane dla słowa '{slowo}' zostały zapisane do tabeli 'dane_w_czasie'.")
            else:
                # Wysłanie e-maila w przypadku braku danych
                subject = f"Brak danych o zainteresowaniu dla słowa '{slowo}'"
                message = f"Brak danych o zainteresowaniu w czasie dla słowa '{slowo}'"
                send_email(subject, message, recipient_email)
                print(f"Brak danych o zainteresowaniu w czasie dla słowa '{slowo}'.")

    except Exception as e:
        # Wysłanie e-maila w przypadku błędu
        subject = "Błąd podczas pobierania danych o zainteresowaniu w czasie"
        message = f"Błąd podczas pobierania danych o zainteresowaniu w czasie: {e}"
        send_email(subject, message, recipient_email)
        print(f"Błąd podczas pobierania danych o zainteresowaniu w czasie: {e}")

    finally:
        # Zamknięcie kursora i połączenia z bazą danych
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        print("Połączenie z bazą danych zostało zamknięte.")
 