import json
import logging
import os

from confluent_kafka import Producer, Consumer, KafkaException
from sentence_transformers import SentenceTransformer

local_model_path = "../model"

if not os.path.exists(local_model_path):
    print("Скачивание модели...")
    model = SentenceTransformer('jinaai/jina-embeddings-v3', trust_remote_code=True)
    model.save(local_model_path)
    print("Модель сохранена")

# Загружаем модель
vectorizer = SentenceTransformer(local_model_path)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def generate_embedding(query):
    return vectorizer.encode(query).tolist()

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
logging.info(f"Consumer started")
try:
    while True:
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
