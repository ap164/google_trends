import email
from email import policy
from email.parser import BytesParser
from imapclient import IMAPClient
from imapclient import exceptions as imap_exceptions
from confluent_kafka import Producer
import socket
import time
import sys
import os
import ssl
import certifi
import traceback
import logging

# Ustawienie logowania
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Konfiguracja Kafki
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
if not KAFKA_BROKER:
    logger.error("Zmienna środowiskowa KAFKA_BROKER nie jest ustawiona.")
    sys.exit(1)
TOPIC = 'email_csv_topic'

# Dane logowania do konta e-mail
USERNAME = os.getenv('EMAIL_USERNAME')
PASSWORD = os.getenv('EMAIL_PASSWORD')

if USERNAME is None or PASSWORD is None:
    logger.error("Zmienna środowiskowa EMAIL_USERNAME lub EMAIL_PASSWORD nie jest ustawiona.")
    sys.exit(1)

# Kryteria wyszukiwania
SUBJECT_KEYWORD = "qwerty"

def delivery_report(err, msg):
    """Funkcja wywoływana po dostarczeniu wiadomości lub wystąpieniu błędu."""
    if err is not None:
        logger.error(f'Błąd dostarczenia wiadomości: {err}')
    else:
        logger.info(f'Wiadomość dostarczona do {msg.topic()} [{msg.partition()}]')

def main():
    # Konfiguracja producenta Kafki
    producer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': socket.gethostname(),
    }
    try:
        producer = Producer(producer_conf)
        logger.info("Producent Kafka skonfigurowany pomyślnie.")
    except Exception as e:
        logger.error(f"Błąd podczas konfiguracji producenta Kafki: {e}")
        traceback.print_exc()
        sys.exit(1)

    # Połączenie z serwerem IMAP
    try:
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        server = IMAPClient('imap.gmx.com', ssl=True, ssl_context=ssl_context)
        server.login(USERNAME, PASSWORD)
        server.select_folder('INBOX')
        logger.info("Zalogowano się do skrzynki odbiorczej.")
    except imap_exceptions.LoginError as e:
        logger.error(f"Nie udało się zalogować: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Nie udało się połączyć z serwerem IMAP: {e}")
        traceback.print_exc()
        sys.exit(1)

    # Nasłuchiwanie na nowe wiadomości
    try:
        logger.info("Rozpoczynam nasłuchiwanie na nowe wiadomości...")
        while True:
            try:
                # Rozpocznij tryb IDLE
                server.idle()
                # Oczekuj powiadomienia przez 30 sekund
                responses = server.idle_check(timeout=30)
                # Zakończ tryb IDLE
                server.idle_done()

                # Po otrzymaniu powiadomienia sprawdź nowe wiadomości
                if responses:
                    logger.info("Otrzymano powiadomienie o nowych wiadomościach.")
                    # Wyszukaj nieprzeczytane wiadomości zawierające słowo "raport" w temacie
                    messages = server.search(['UNSEEN', 'SUBJECT', SUBJECT_KEYWORD])
                    if messages:
                        logger.info(f"Znaleziono {len(messages)} nowych wiadomości zawierających '{SUBJECT_KEYWORD}' w temacie.")
                        for msg_id in messages:
                            try:
                                # Pobierz pełne dane wiadomości
                                raw_message = server.fetch(msg_id, ['RFC822'])
                                raw_bytes = raw_message[msg_id][b'RFC822']
                                message = BytesParser(policy=policy.default).parsebytes(raw_bytes)
                                subject = message['subject']
                                from_email = message['from']
                                logger.info(f"Przetwarzanie wiadomości ID {msg_id}: {subject} od {from_email}")

                                # Wyodrębnij treść wiadomości
                                if message.is_multipart():
                                    body = ""
                                    for part in message.iter_parts():
                                        if part.get_content_type() == 'text/plain':
                                            body += part.get_content()
                                else:
                                    body = message.get_content()

                                # Wyślij treść wiadomości do Kafki
                                producer.produce(TOPIC, body.encode('utf-8'), callback=delivery_report)
                                producer.poll(0)

                                # Oznacz wiadomość jako przeczytaną
                                server.add_flags(msg_id, [b'\\Seen'])
                            except Exception as e:
                                logger.error(f"Błąd podczas przetwarzania wiadomości ID {msg_id}: {e}")
                                traceback.print_exc()
                    else:
                        logger.info("Brak nowych wiadomości do przetworzenia.")
                else:
                    logger.debug("Timeout: brak nowych wiadomości.")
            except Exception as e:
                logger.error(f"Wystąpił błąd podczas nasłuchiwania IDLE: {e}")
                traceback.print_exc()
                time.sleep(5)  # Opcjonalnie: poczekaj przed ponowną próbą
    except KeyboardInterrupt:
        logger.info("Przerwano działanie skryptu.")
    except Exception as e:
        logger.error(f"Wystąpił błąd: {e}")
        traceback.print_exc()
    finally:
        server.logout()
        producer.flush()
        logger.info("Wylogowano się z serwera IMAP i zamknięto producenta Kafka.")

if __name__ == '__main__':
    main()
