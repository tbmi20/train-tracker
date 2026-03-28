from confluent_kafka import Consumer, KafkaException, KafkaError
from dotenv import load_dotenv
import os
from pathlib import Path

from services.observation import MessageParser, LocationMapper, Observer


def main():
    try:
        load_dotenv()

        conf = {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL"),
            "sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM"),
            "sasl.username": os.getenv("CONSUMER_USERNAME"),
            "sasl.password": os.getenv("CONSUMER_PASSWORD"),
            "group.id": os.getenv("CONSUMER_GROUP"),
            "auto.offset.reset": "earliest",  # Set to earliest to get messages for trains which have already moved
            "enable.metrics.push": False,
        }
        consumer = Consumer(conf)
        topic = str(os.getenv("JSON_TOPIC"))
        corpus_path = Path(str(os.getenv("TIPLOC_CORPUS_PATH")))

        tiploc_mapper = LocationMapper(corpus_path)
        message_parser = MessageParser(tiploc_mapper)

        # Create the Observer and start consuming messages
        observer = Observer(consumer, topic, message_parser)
        observer.consume()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
