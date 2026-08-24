from confluent_kafka import Consumer
from dotenv import load_dotenv
import os
from pathlib import Path
import logging

from services.database import Database
from services.observation import MessageParser, Observer
from services.models import Watchlist

log_folder = Path("logs")
log_folder.mkdir(exist_ok=True)
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(
    filename=log_folder / "app.log", level=logging.INFO, format=log_format
)
logger = logging.getLogger(__name__)


def main():
    """Always-on live ingest entrypoint.

    Deliberately does *not* load the weekly timetable - that's Airflow's
    job (nightly_schedule_download), run independently of this process.
    Runs indefinitely; the watchlist itself is refreshed periodically from
    Postgres inside Observer.consume() so watches added via the API take
    effect without restarting this process.
    """
    try:
        load_dotenv()

        database = Database(str(os.getenv("DB_PATH")))
        database.initialise_schema()

        watchlist = Watchlist.from_db(database)

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
        observer = Observer(consumer, topic, message_parser, watchlist, database)
        observer.consume()

    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
