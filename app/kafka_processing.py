import json
import logging
from confluent_kafka import Producer, Consumer, KafkaException

from vectorizer import generate_embedding

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

kafka_config = {
    "bootstrap.servers": "kafka:9092",
    "group.id": "embedding_group",
    "auto.offset.reset": "earliest"
}

producer = Producer({"bootstrap.servers": kafka_config["bootstrap.servers"]})


def delivery_report(err, msg):
    """Функция обратного вызова для подтверждения отправки сообщения"""
    if err:
        logging.info(f"Ошибка доставки: {err}")
    else:
        logging.info(f"Сообщение отправлено: {msg.topic()} [{msg.partition()}]")


consumer = Consumer(kafka_config)
consumer.subscribe(["vector"])

try:
    while True:
        logging.info(f"Consumer started")
        msg = consumer.poll(timeout=0.5)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        # Декодируем JSON-сообщение
        received_message = json.loads(msg.value().decode("utf-8"))
        logging.info(f"Получено сообщение: {received_message}")

        processed_embedding = generate_embedding(received_message["embedding"])
        logging.info(f"Обработанный embedding: {processed_embedding}")

        producer.produce(
            "vector_res",
            key=msg.key(),
            value=json.dumps(received_message).encode("utf-8"),
            callback=delivery_report
        )
        producer.flush()
        logging.info("Сообщение отправлено обратно")

except KeyboardInterrupt:
    logging.info("Остановка консюмера")
finally:
    consumer.close()
