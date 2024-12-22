from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import os
import logging

# Ustawienie logowania
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Konfiguracja konsumenta
    KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
    if not KAFKA_BROKER:
        logger.error("Zmienna środowiskowa KAFKA_BROKER nie jest ustawiona.")
        sys.exit(1)

    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'my-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }

    try:
        consumer = Consumer(conf)
        logger.info("Konsument Kafka skonfigurowany pomyślnie.")
    except KafkaException as e:
        logger.error(f"Błąd podczas konfiguracji konsumenta Kafka: {e}")
        sys.exit(1)

    topic = 'email_csv_topic'

    try:
        consumer.subscribe([topic])
        logger.info(f"Subskrybowano temat: {topic}")

        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Osiągnięto koniec partycji {msg.topic()} [{msg.partition()}]")
                else:
                    logger.error(f"Błąd: {msg.error()}")
                continue

            logger.info(f'Odebrano wiadomość: {msg.value().decode("utf-8")} z tematu {msg.topic()}')

    except KeyboardInterrupt:
        logger.info("Przerwano działanie konsumenta.")
    except Exception as e:
        logger.error(f'Wystąpił błąd: {e}')
    finally:
        consumer.close()
        logger.info("Zamknięto konsumenta Kafka.")

if __name__ == '__main__':
    main()
