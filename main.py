from confluent_kafka import Consumer, KafkaException, KafkaError
from dotenv import load_dotenv
import os
from pathlib import Path
import logging

from services.load_user_settings import load_user_settings
from services.observation import MessageParser, LocationMapper, Observer, Watchlist

log_folder = Path("logs")
log_folder.mkdir(exist_ok=True)
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(
    filename=log_folder / "app.log", level=logging.INFO, format=log_format
)
logger = logging.getLogger(__name__)


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

        user_settings = load_user_settings(str(os.getenv("USER_SETTINGS_PATH")))

        watchlist = Watchlist(
            user_settings.get("saved_trains", []),
            user_settings.get("favourite_stations", []),
        )
        # Create the Observer and start consuming messages
        observer = Observer(consumer, topic, message_parser, watchlist)
        observer.consume()

    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
