from confluent_kafka import Consumer, KafkaException, KafkaError
import os, sys
import json
from pathlib import Path
from enum import Enum
from dataclasses import dataclass
from pydantic import ValidationError

from logging import getLogger

logger = getLogger(__name__)
from services.models import TrainUpdate, TrainLocation


@dataclass
class Journey:
    uid: str
    rid: str
    location_history: list[TrainLocation]


class EventType(Enum):
    """Enum for train event types.

    PASS: Train is passing through the location without stopping.
    STOP: Train is stopping at the location.
    """

    PASS = "PASS"
    STOP = "STOP"


class LocationMapper:
    """Mapper for converting TIPLOC codes to human-readable location names."""

    def __init__(self, corpus_path: str | Path):
        self.corpus_path = corpus_path
        self.tiploc_data = self.load_corpus()
        # TODO: Create caching dictionary for faster lookups

    def load_corpus(self):
        corpus = json.load(open(self.corpus_path))
        tiploc_data = corpus.get("TIPLOCDATA", [])

        if not tiploc_data:
            raise ValueError("TIPLOCDATA not found in corpus")
        return tiploc_data

    def tiploc_to_location(self, tiploc) -> str | None:
        """Mapper for converting TIPLOC codes to human-readable location names."""
        for tiploc_dict in self.tiploc_data:
            if tiploc_dict.get("TIPLOC") == tiploc:
                return tiploc_dict.get("NLCDESC", None)
        return None


class MessageParser:
    """Converts Kafka message to TrainUpdate objects, using a LocationMapper to resolve TIPLOC codes to location names."""

    def __init__(self, tiploc_mapper: LocationMapper):
        self.tiploc_mapper = tiploc_mapper

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


class Watchlist:
    """Maintains watchlists for user subscriptions."""

    def __init__(
        self, subscribed_trains: list[str], favourite_stations: list[str] = []
    ):
        self.station_watchlist: dict[str, list[TrainUpdate]] = {
            station: [] for station in favourite_stations
        }
        self.train_watchlist: dict[str, Journey] = {
            uid: Journey(uid=uid, rid="", location_history=[])
            for uid in subscribed_trains
        }

    def update_watchlists(self, train_update: TrainUpdate):
        """Updates the watchlists with the latest train update information."""

        if train_update.uid in self.train_watchlist:  # live train update
            # Update existing entry or create a new one
            self.train_watchlist[train_update.uid].rid = train_update.rid
            self.train_watchlist[train_update.uid].location_history.extend(
                train_update.location
            )  # Append new location info
            return self.train_watchlist[
                train_update.uid
            ]  # Read-only Journey object for external use

        stations_intersect = set([loc.tiploc for loc in train_update.location]) & set(
            self.station_watchlist.keys()
        )  # station update for a train we're not tracking, but we care about the station
        if stations_intersect:
            for loc in stations_intersect:
                self.station_watchlist[loc].append(train_update)
                print(
                    f"Train {train_update.rid} ({train_update.uid}) has an update for station {loc}"
                )


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
                    train_event = self.processor.parse_message(msg)
                    if train_event is None:
                        continue
                    self.watchlist.update_watchlists(train_event)
        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()

    def shutdown(self):
        self.running = False
