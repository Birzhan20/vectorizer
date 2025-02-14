import json
from confluent_kafka import Producer, Consumer, KafkaException

from vectorizer import generate_query_embedding

kafka_config = {
    "bootstrap.servers": "kafka:9092",
    "group.id": "test_group",
    "auto.offset.reset": "earliest"
}

producer = Producer({"bootstrap.servers": kafka_config["bootstrap.servers"]})


def delivery_report(err, msg):
    """Функция обратного вызова для подтверждения отправки сообщения"""
    if err:
        print(f"Ошибка доставки: {err}")
    else:
        print(f"Сообщение отправлено: {msg.topic()} [{msg.partition()}]")


consumer = Consumer(kafka_config)
consumer.subscribe(["vector"])

try:
    while True:
        msg = consumer.poll(timeout=0.5)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        # Декодируем JSON-сообщение
        received_message = json.loads(msg.value().decode("utf-8"))
        print(f"Получено сообщение: {received_message}")

        processed_embedding = generate_query_embedding(received_message["embedding"])
        print(f"Обработанный embedding: {processed_embedding}")

        producer.produce(
            "vector_res",
            key=msg.key(),
            value=json.dumps(received_message).encode("utf-8"),
            callback=delivery_report
        )
        producer.flush()

except KeyboardInterrupt:
    print("Остановка консюмера")
finally:
    consumer.close()
