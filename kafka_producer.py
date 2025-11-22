import random
import calendar
import time
import json

from typing import TypedDict
from kafka import KafkaProducer


class Event(TypedDict):
    user_id: int
    movie_id: int
    event: str
    tz: int


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def new_fake_event() -> Event:
    return {
        "user_id": random.randint(1, 1_000_000),
        "movie_id": random.randint(1, 100),
        "event": random.choice(["PLAY", "PAUSE", "COMPLETE", "HOVER"]),
        "tz": calendar.timegm(time.gmtime()),
    }


for i in range(1, 10):
    event = new_fake_event()
    producer.send("spark-topic-1", new_fake_event())

producer.flush()
