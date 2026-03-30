"""
Unit tests for the event producer (event_producer/producer.py).

Kafka is NOT needed — we mock the confluent_kafka.Producer so these tests run
without any infrastructure.
"""

from __future__ import annotations

import importlib
import sys
import os
import uuid
from unittest.mock import MagicMock, call, patch

import pytest

# Make event_producer importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "event_producer"))

# Stub confluent_kafka before importing producer
_ck_mock = MagicMock()
sys.modules.setdefault("confluent_kafka", _ck_mock)

import producer  # noqa: E402  (import after sys.path manipulation)
from schemas import ListenEvent, Location, PageViewEvent, AuthEvent, Song, User


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture()
def sample_user() -> User:
    return User(
        user_id=str(uuid.uuid4()),
        username="testuser",
        email="test@example.com",
        age=30,
        gender="F",
        country="Germany",
        city="Berlin",
        subscription_level="premium",
        registration_date="2023-06-01",
    )


@pytest.fixture()
def sample_song() -> Song:
    return Song(
        song_id=str(uuid.uuid4()),
        song_name="Test Song",
        artist_name="Test Artist",
        album_name="Test Album",
        genre="Electronic",
        release_year=2020,
        duration_ms=240_000,
    )


# ── Catalogue builders ────────────────────────────────────────────────────────


class TestCatalogueBuilders:
    def test_build_users_count(self) -> None:
        users = producer._build_users(10)
        assert len(users) == 10

    def test_build_users_types(self) -> None:
        users = producer._build_users(5)
        for u in users:
            assert isinstance(u, User)
            assert u.subscription_level in ("free", "premium")
            assert 13 <= u.age <= 65

    def test_build_songs_count(self) -> None:
        songs = producer._build_songs(20)
        assert len(songs) == 20

    def test_build_songs_duration_range(self) -> None:
        songs = producer._build_songs(50)
        for s in songs:
            assert 90_000 <= s.duration_ms <= 420_000

    def test_build_songs_valid_genre(self) -> None:
        songs = producer._build_songs(50)
        valid = {
            "Pop", "Rock", "Hip-Hop", "Electronic", "R&B",
            "Country", "Jazz", "Classical", "Latin", "Indie",
        }
        for s in songs:
            assert s.genre in valid


# ── Event factories ───────────────────────────────────────────────────────────


class TestEventFactories:
    def test_make_listen_event_type(self, sample_user: User, sample_song: Song) -> None:
        ev = producer.make_listen_event(sample_user, sample_song)
        assert isinstance(ev, ListenEvent)
        assert ev.event_type == "listen"

    def test_make_listen_event_ms_played_lte_duration(
        self, sample_user: User, sample_song: Song
    ) -> None:
        ev = producer.make_listen_event(sample_user, sample_song)
        assert ev.ms_played <= sample_song.duration_ms

    def test_make_listen_event_location_country(
        self, sample_user: User, sample_song: Song
    ) -> None:
        ev = producer.make_listen_event(sample_user, sample_song)
        assert ev.location.country == sample_user.country

    def test_make_page_view_event_type(self, sample_user: User) -> None:
        ev = producer.make_page_view_event(sample_user)
        assert isinstance(ev, PageViewEvent)
        assert ev.event_type == "page_view"

    def test_make_page_view_event_page_valid(self, sample_user: User) -> None:
        for _ in range(20):
            ev = producer.make_page_view_event(sample_user)
            assert ev.page in producer.PAGES

    def test_make_page_view_prev_page_passed(self, sample_user: User) -> None:
        ev = producer.make_page_view_event(sample_user, prev_page="home")
        assert ev.prev_page == "home"

    def test_make_auth_event_type(self, sample_user: User) -> None:
        ev = producer.make_auth_event(sample_user)
        assert isinstance(ev, AuthEvent)
        assert ev.event_type == "auth"

    def test_make_auth_event_action_valid(self, sample_user: User) -> None:
        for _ in range(20):
            ev = producer.make_auth_event(sample_user)
            assert ev.action in producer.AUTH_ACTIONS

    def test_make_auth_event_method_valid(self, sample_user: User) -> None:
        for _ in range(20):
            ev = producer.make_auth_event(sample_user)
            assert ev.method in producer.AUTH_METHODS


# ── Producer send helper ──────────────────────────────────────────────────────


class TestSendHelper:
    def test_send_encodes_payload_as_json(self, sample_user: User, sample_song: Song) -> None:
        mock_producer = MagicMock()
        ev = producer.make_listen_event(sample_user, sample_song)
        producer._send(mock_producer, producer.TOPIC_LISTEN, sample_user.user_id, ev.to_dict())

        mock_producer.produce.assert_called_once()
        kwargs = mock_producer.produce.call_args.kwargs
        assert kwargs["topic"] == producer.TOPIC_LISTEN
        assert kwargs["key"] == sample_user.user_id.encode("utf-8")
        # value must be valid JSON bytes
        import json
        parsed = json.loads(kwargs["value"].decode("utf-8"))
        assert parsed["event_type"] == "listen"

    def test_send_uses_correct_topic(self, sample_user: User) -> None:
        mock_producer = MagicMock()
        ev = producer.make_auth_event(sample_user)
        producer._send(mock_producer, producer.TOPIC_AUTH, sample_user.user_id, ev.to_dict())
        kwargs = mock_producer.produce.call_args.kwargs
        assert kwargs["topic"] == producer.TOPIC_AUTH
