from confluent_kafka import Consumer, KafkaException, KafkaError
import socket
from dotenv import load_dotenv
import os
import sys
import json

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
                process_message(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def shutdown():
    running = False


def process_message(kafka_msg):
    # 1. Parse the outer wrapper
    data = json.loads(kafka_msg.value())

    # 2. Extract and parse the inner 'bytes' string
    inner_payload = json.loads(data["bytes"])

    # 3. Access the 'uR' (Update Record)
    update = inner_payload.get("uR", {})

    if "TS" in update:
        train_id = update["TS"]["rid"]
        location_data = update["TS"].get("Location", {})
        if isinstance(location_data, list):
            tiploc = location_data[0].get("tpl") if location_data else "Unknown"
        else:
            tiploc = location_data.get("tpl")

        # Check if it's an arrival, departure, or pass
        event_type = "PASS" if "pass" in location_data else "STOP"

        print(f"Train {train_id} just {event_type}ed {tiploc}")


if __name__ == "__main__":
    basic_consume_loop(consumer, [topic])
