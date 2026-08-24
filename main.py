from confluent_kafka import Consumer, KafkaException, KafkaError
from dotenv import load_dotenv
import os
from pathlib import Path
import logging

from services.load_data import load_user_settings, load_timetable
from services.database import Database
from services.observation import MessageParser, Observer, Watchlist

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

        # TODO: Eventually separate this so the rest of the app runs while user settings and timetables are refreshed
        # Get environment variables for timetable
        timetable_path = str(os.getenv("TIMETABLE_PATH"))
        database = Database(str(os.getenv("DB_PATH")))
        database.initialise_schema()

        # Load tiploc map and schedule from the weekly timetable into db
        load_timetable(timetable_path, database)

        # Get user specified settings for saved trains and favourite stations
        user_settings_path = str(os.getenv("USER_SETTINGS_PATH"))
        user_settings = load_user_settings(user_settings_path)
        watchlist = Watchlist(
            subscribed_trains=[train.uid for train in user_settings.saved_trains],
            favourite_stations=user_settings.favourite_stations,
        )

        # Set up Kafka consumer configuration

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

        message_parser = MessageParser()

        # Create the Observer and start consuming messages
        observer = Observer(consumer, topic, message_parser, watchlist)
        observer.consume()

    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
