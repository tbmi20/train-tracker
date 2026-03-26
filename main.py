from confluent_kafka import Consumer, KafkaException, KafkaError
import socket
from dotenv import load_dotenv
import os
import sys

load_dotenv()

conf = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL"),
    "sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM"),
    "sasl.username": os.getenv("CONSUMER_USERNAME"),
    "sasl.password": os.getenv("CONSUMER_PASSWORD"),
    "group.id": os.getenv("CONSUMER_GROUP"),
    "auto.offset.reset": "earliest",
}

consumer = Consumer(conf)

topic = os.getenv("JSON_TOPIC")
consumer.subscribe([topic]) if topic else print("Topic set incorrectly")

running = True


def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write(
                        "%% %s [%d] reached end at offset %d\n"
                        % (msg.topic(), msg.partition(), msg.offset())
                    )
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                key = msg.key().decode("utf-8") if msg.key() else None
                value = msg.value().decode("utf-8") if msg.value() else None
                print(f"Consumed message: key={key}, value={value}")
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def shutdown():
    running = False


if __name__ == "__main__":
    basic_consume_loop(consumer, [topic])
