from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import json
import time
from datetime import datetime, timezone
from pydantic import ValidationError

from logging import getLogger

logger = getLogger(__name__)
from services.database import Database
from services.models import TrainUpdate, Watchlist

# How often the ingest loop reloads watchlist_stations/watchlist_trains from
# Postgres, so watches added/removed via the API get picked up without
# restarting this process.
WATCHLIST_REFRESH_SECONDS = 120


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

        # ssd (schedule date) is provisionally read off the parent uR record
        # if TS itself doesn't carry it - unconfirmed against real Darwin
        # messages, TS's own value (if present) wins.
        ts_data = {"ssd": update_record.get("ssd"), **ts_data}

        try:
            return TrainUpdate(**ts_data)
        except ValidationError as exc:
            logger.warning(f"Skipping malformed train update: {exc}")
            return None


class Observer:
    """Consumes Kafka messages, filters to the watchlist, and persists matches to Postgres."""

    def __init__(
        self,
        consumer: Consumer,
        topic: str,
        processor: MessageParser,
        watchlist: Watchlist,
        database: Database,
    ):
        self.consumer = consumer
        self.topic = topic
        self.running = True

        self.processor = processor
        self.watchlist = watchlist
        self.database = database

    def subscribe(self):
        if self.topic:
            self.consumer.subscribe([self.topic])
        else:
            print("Topic set incorrectly")

    def _persist(self, train_update: TrainUpdate) -> None:
        ssd = train_update.ssd or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if not self.database.upsert_live_journey(
            rid=train_update.rid, uid=train_update.uid, ssd=ssd
        ):
            return  # already logged by Database; don't attempt the events below

        for location in train_update.location:
            self.database.upsert_live_journey_event(
                rid=train_update.rid,
                tiploc=location.tiploc,
                planned_arr=location.planned_arr,
                planned_dep=location.planned_dep,
                est_arr=location.est_arr,
                act_arr=location.act_arr,
                platform=location.platform,
            )

    def consume(self):
        """Consumes messages from Kafka and processes them. Stops when shutdown is called.

        Raises:
            KafkaException: If there is an error while consuming messages from Kafka.
        """
        last_refresh = time.monotonic()
        try:
            self.subscribe()
            while self.running:
                if time.monotonic() - last_refresh >= WATCHLIST_REFRESH_SECONDS:
                    self.watchlist.refresh(self.database)
                    last_refresh = time.monotonic()

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
                    if not self.watchlist.matches(msg):
                        continue
                    train_event = self.processor.parse_message(msg)
                    if train_event is None:
                        continue
                    self._persist(train_event)
        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()

    def shutdown(self):
        self.running = False
