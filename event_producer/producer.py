"""
Fake music-streaming event producer.

Generates realistic listen / page-view / auth events using the Faker library
and the pre-built user/song catalogues, then publishes them to three separate
Kafka topics:

  * listen_events
  * page_view_events
  * auth_events

Configuration is read from environment variables (see .env.example).
"""

from __future__ import annotations

import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from typing import List

from confluent_kafka import Producer
from faker import Faker

from schemas import AuthEvent, ListenEvent, Location, PageViewEvent, Song, User

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

fake = Faker()
Faker.seed(42)
random.seed(42)

# ── Configuration ────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EVENTS_PER_SECOND: float = float(os.getenv("EVENTS_PER_SECOND", "10"))
NUM_USERS: int = int(os.getenv("NUM_USERS", "1000"))
NUM_SONGS: int = int(os.getenv("NUM_SONGS", "500"))

TOPIC_LISTEN = "listen_events"
TOPIC_PAGE_VIEW = "page_view_events"
TOPIC_AUTH = "auth_events"

PAGES = ["home", "search", "library", "artist", "album", "playlist", "radio", "profile"]
PLATFORMS = ["web", "ios", "android"]
AUTH_METHODS = ["email", "google", "facebook"]
AUTH_ACTIONS = ["login", "logout", "register"]
GENRES = [
    "Pop", "Rock", "Hip-Hop", "Electronic", "R&B",
    "Country", "Jazz", "Classical", "Latin", "Indie",
]

# ── Catalogue generation ──────────────────────────────────────────────────────


def _build_users(n: int) -> List[User]:
    logger.info("Building user catalogue (%d users)…", n)
    users = []
    for _ in range(n):
        users.append(
            User(
                user_id=str(uuid.uuid4()),
                username=fake.user_name(),
                email=fake.email(),
                age=random.randint(13, 65),
                gender=random.choice(["M", "F", "Other"]),
                country=fake.country(),
                city=fake.city(),
                subscription_level=random.choice(["free", "free", "premium"]),
                registration_date=fake.date_between(
                    start_date="-3y", end_date="today"
                ).isoformat(),
            )
        )
    return users


def _build_songs(n: int) -> List[Song]:
    logger.info("Building song catalogue (%d songs)…", n)
    songs = []
    for _ in range(n):
        duration_ms = random.randint(90_000, 420_000)
        songs.append(
            Song(
                song_id=str(uuid.uuid4()),
                song_name=" ".join(fake.words(nb=random.randint(1, 4))).title(),
                artist_name=fake.name(),
                album_name=" ".join(fake.words(nb=random.randint(1, 3))).title(),
                genre=random.choice(GENRES),
                release_year=random.randint(1960, 2026),
                duration_ms=duration_ms,
            )
        )
    return songs


# ── Event factories ──────────────────────────────────────────────────────────


def make_listen_event(user: User, song: Song) -> ListenEvent:
    ms_played = random.randint(10_000, song.duration_ms)
    return ListenEvent(
        user_id=user.user_id,
        song_id=song.song_id,
        song_name=song.song_name,
        artist_name=song.artist_name,
        album_name=song.album_name,
        genre=song.genre,
        duration_ms=song.duration_ms,
        ms_played=ms_played,
        platform=random.choice(PLATFORMS),
        location=Location(
            country=user.country,
            city=user.city,
            latitude=float(fake.latitude()),
            longitude=float(fake.longitude()),
        ),
    )


def make_page_view_event(user: User, prev_page: str | None = None) -> PageViewEvent:
    page = random.choice(PAGES)
    return PageViewEvent(
        user_id=user.user_id,
        session_id=str(uuid.uuid4()),
        page=page,
        prev_page=prev_page,
        platform=random.choice(PLATFORMS),
    )


def make_auth_event(user: User) -> AuthEvent:
    return AuthEvent(
        user_id=user.user_id,
        action=random.choice(AUTH_ACTIONS),
        success=random.choices([True, False], weights=[9, 1])[0],
        method=random.choice(AUTH_METHODS),
        platform=random.choice(PLATFORMS),
    )


# ── Kafka helpers ─────────────────────────────────────────────────────────────


def _delivery_report(err, msg):
    if err is not None:
        logger.error("Delivery failed for key %s: %s", msg.key(), err)


def _make_producer() -> Producer:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
    }
    return Producer(conf)


def _send(producer: Producer, topic: str, key: str, payload: dict) -> None:
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(payload).encode("utf-8"),
        callback=_delivery_report,
    )


# ── Main loop ────────────────────────────────────────────────────────────────


def run() -> None:
    users = _build_users(NUM_USERS)
    songs = _build_songs(NUM_SONGS)
    producer = _make_producer()

    # Graceful shutdown
    running = True

    def _stop(signum, frame):  # noqa: ANN001
        nonlocal running
        logger.info("Shutdown signal received, flushing producer…")
        running = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    sleep_interval = 1.0 / EVENTS_PER_SECOND
    total_produced = 0

    # Weights: listen events are far more common than auth or page-view events
    event_weights = [0.70, 0.20, 0.10]  # listen, page_view, auth
    event_types = ["listen", "page_view", "auth"]

    logger.info(
        "Starting producer → %s  (%.1f events/s)",
        KAFKA_BOOTSTRAP_SERVERS,
        EVENTS_PER_SECOND,
    )

    prev_pages: dict[str, str | None] = {}

    while running:
        user = random.choice(users)
        event_type = random.choices(event_types, weights=event_weights)[0]

        if event_type == "listen":
            song = random.choice(songs)
            event = make_listen_event(user, song)
            _send(producer, TOPIC_LISTEN, user.user_id, event.to_dict())

        elif event_type == "page_view":
            prev = prev_pages.get(user.user_id)
            event = make_page_view_event(user, prev_page=prev)
            prev_pages[user.user_id] = event.page
            _send(producer, TOPIC_PAGE_VIEW, user.user_id, event.to_dict())

        else:  # auth
            event = make_auth_event(user)
            _send(producer, TOPIC_AUTH, user.user_id, event.to_dict())

        total_produced += 1
        if total_produced % 1000 == 0:
            producer.poll(0)
            logger.info("Produced %d events so far", total_produced)

        time.sleep(sleep_interval)

    producer.flush()
    logger.info("Producer shut down cleanly after %d events.", total_produced)
    sys.exit(0)


if __name__ == "__main__":
    run()
