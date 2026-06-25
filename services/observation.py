from confluent_kafka import Consumer, KafkaException, KafkaError
import os, sys
import json
from pathlib import Path
from pydantic import ValidationError

from logging import getLogger

logger = getLogger(__name__)
from services.models import TrainUpdate, TrainLocation, Watchlist


class MessageParser:
    """Converts Kafka message to TrainUpdate objects."""

    def __init__(self):
        pass

    def parse_message(self, kafka_msg) -> TrainUpdate | None:
        """Parses a Kafka message and extracts train ID, tiploc, and event type packaged as a TrainUpdate."""
        # 1. Parse the outer wrapper
        data = json.loads(kafka_msg.value())

        # 2. Extract and parse the inner 'bytes' string
        inner_payload = json.loads(data["bytes"])

        # 3. Access the 'uR' (Update Record)
        update_record = inner_payload.get("uR", {})

        # 4. Get the Train Status (ts) data
        ts_data = update_record.get("TS", {})
        if not ts_data:
            logger.info("No TS data found in message, skipping.")
            return None

        try:
            return TrainUpdate(**ts_data)
        except ValidationError as exc:
            logger.warning(f"Skipping malformed train update: {exc}")
            return None


class Observer:
    """Observer for consuming Kafka messages and processing train information."""

    def __init__(
        self,
        consumer: Consumer,
        topic: str,
        processor: MessageParser,
        watchlist: Watchlist,
    ):
        self.consumer = consumer
        self.topic = topic
        self.running = True

        self.processor = processor
        self.watchlist = watchlist

    def subscribe(self):
        if self.topic:
            self.consumer.subscribe([self.topic])
        else:
            print("Topic set incorrectly")

    def consume(self):
        """Consumes messages from Kafka and processes them. Stops when shutdown is called.

        Raises:
            KafkaException: If there is an error while consuming messages from Kafka.
        """
        try:
            self.subscribe()
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                msg_error = msg.error()
                if msg_error is not None:
                    if msg_error.code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write(
                            "%% %s [%d] reached end at offset %d\n"
                            % (msg.topic(), msg.partition(), msg.offset())
                        )
                    elif msg_error:
                        raise KafkaException(msg_error)
                else:
                    print(f"Received message: {msg.value()}")
                    train_event = self.processor.parse_message(msg)
                    if train_event is None:
                        continue
                    self.watchlist.update_watchlists(train_event)
        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()

    def shutdown(self):
        self.running = False
