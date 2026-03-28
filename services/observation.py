from confluent_kafka import Consumer, KafkaException, KafkaError
import os, sys
import json
from pathlib import Path
from enum import Enum
from dataclasses import dataclass


@dataclass
class TrainEvent:
    train_id: str
    tiploc: str | None
    location_name: str | None
    event_type: EventType
    timestamp: str


class EventType(Enum):
    PASS = "PASS"
    STOP = "STOP"


class MessageParser:
    """Parser for Kafka messages containing train information."""

    def __init__(self, tiploc_mapper: LocationMapper):
        self.tiploc_mapper = tiploc_mapper

    def parse_message(self, kafka_msg) -> TrainEvent | None:
        """Parses a Kafka message and extracts train ID, tiploc, and event type."""
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
                tiploc = location_data[0].get("tpl") if location_data else None
            else:
                tiploc = location_data.get("tpl")

            # Check if it's an arrival/departure, or pass
            event_type = EventType.PASS if "pass" in location_data else EventType.STOP
            location_name = (
                self.tiploc_mapper.tiploc_to_location(tiploc) if tiploc else None
            )

            return TrainEvent(
                train_id=train_id,
                tiploc=tiploc,
                location_name=location_name,
                event_type=event_type,
                timestamp=data.get("timestamp", ""),
            )

        return None


class LocationMapper:
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


class Observer:
    """Observer for consuming Kafka messages and processing train information."""

    def __init__(self, consumer: Consumer, topic: str, processor: MessageParser):
        self.consumer = consumer
        self.topic = topic
        self.running = True

        self.processor = processor

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
                    print(train_event)
        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()

    def shutdown(self):
        self.running = False
