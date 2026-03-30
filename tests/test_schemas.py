"""
Unit tests for event schemas (event_producer/schemas.py).

These tests verify that every event type:
  1. Can be instantiated with valid arguments.
  2. Serialises to a dict that contains the required top-level keys.
  3. Has the correct ``event_type`` sentinel value.
  4. Generates a fresh UUID for ``event_id`` on every construction.
  5. Generates a non-empty ISO-8601 ``timestamp`` string.
"""

from __future__ import annotations

import sys
import os
import uuid
from datetime import datetime, timezone

import pytest

# Make event_producer importable from tests/ without installing it as a package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "event_producer"))

from schemas import AuthEvent, ListenEvent, Location, PageViewEvent, Song, User


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture()
def location() -> Location:
    return Location(country="USA", city="New York", latitude=40.7128, longitude=-74.0060)


@pytest.fixture()
def listen_event(location: Location) -> ListenEvent:
    return ListenEvent(
        user_id=str(uuid.uuid4()),
        song_id=str(uuid.uuid4()),
        song_name="Test Song",
        artist_name="Test Artist",
        album_name="Test Album",
        genre="Pop",
        duration_ms=200_000,
        ms_played=120_000,
        platform="web",
        location=location,
    )


@pytest.fixture()
def page_view_event() -> PageViewEvent:
    return PageViewEvent(
        user_id=str(uuid.uuid4()),
        session_id=str(uuid.uuid4()),
        page="home",
        prev_page=None,
        platform="ios",
    )


@pytest.fixture()
def auth_event() -> AuthEvent:
    return AuthEvent(
        user_id=str(uuid.uuid4()),
        action="login",
        success=True,
        method="email",
        platform="android",
    )


# ── ListenEvent ───────────────────────────────────────────────────────────────


class TestListenEvent:
    REQUIRED_KEYS = {
        "event_type", "event_id", "timestamp",
        "user_id", "song_id", "song_name", "artist_name",
        "album_name", "genre", "duration_ms", "ms_played",
        "platform", "location",
    }

    def test_event_type_sentinel(self, listen_event: ListenEvent) -> None:
        assert listen_event.event_type == "listen"

    def test_to_dict_keys(self, listen_event: ListenEvent) -> None:
        d = listen_event.to_dict()
        assert self.REQUIRED_KEYS.issubset(d.keys())

    def test_event_id_is_uuid(self, listen_event: ListenEvent) -> None:
        # Should not raise
        uuid.UUID(listen_event.event_id)

    def test_event_ids_are_unique(self, location: Location) -> None:
        e1 = ListenEvent(
            user_id="u1", song_id="s1", song_name="A", artist_name="B",
            album_name="C", genre="Pop", duration_ms=200_000, ms_played=100_000,
            platform="web", location=location,
        )
        e2 = ListenEvent(
            user_id="u1", song_id="s1", song_name="A", artist_name="B",
            album_name="C", genre="Pop", duration_ms=200_000, ms_played=100_000,
            platform="web", location=location,
        )
        assert e1.event_id != e2.event_id

    def test_timestamp_is_iso(self, listen_event: ListenEvent) -> None:
        ts = listen_event.timestamp
        # Should end with 'Z' and be parseable
        assert ts.endswith("Z")
        datetime.fromisoformat(ts.rstrip("Z"))

    def test_location_serialised(self, listen_event: ListenEvent) -> None:
        d = listen_event.to_dict()
        loc = d["location"]
        assert loc["country"] == "USA"
        assert loc["city"] == "New York"
        assert isinstance(loc["latitude"], float)
        assert isinstance(loc["longitude"], float)

    def test_ms_played_lte_duration(self, listen_event: ListenEvent) -> None:
        assert listen_event.ms_played <= listen_event.duration_ms


# ── PageViewEvent ─────────────────────────────────────────────────────────────


class TestPageViewEvent:
    REQUIRED_KEYS = {
        "event_type", "event_id", "timestamp",
        "user_id", "session_id", "page", "prev_page", "platform",
    }

    def test_event_type_sentinel(self, page_view_event: PageViewEvent) -> None:
        assert page_view_event.event_type == "page_view"

    def test_to_dict_keys(self, page_view_event: PageViewEvent) -> None:
        d = page_view_event.to_dict()
        assert self.REQUIRED_KEYS.issubset(d.keys())

    def test_prev_page_none_allowed(self, page_view_event: PageViewEvent) -> None:
        assert page_view_event.to_dict()["prev_page"] is None

    def test_prev_page_string_allowed(self) -> None:
        ev = PageViewEvent(
            user_id="u1", session_id="s1", page="search",
            prev_page="home", platform="web",
        )
        assert ev.to_dict()["prev_page"] == "home"


# ── AuthEvent ─────────────────────────────────────────────────────────────────


class TestAuthEvent:
    REQUIRED_KEYS = {
        "event_type", "event_id", "timestamp",
        "user_id", "action", "success", "method", "platform",
    }

    def test_event_type_sentinel(self, auth_event: AuthEvent) -> None:
        assert auth_event.event_type == "auth"

    def test_to_dict_keys(self, auth_event: AuthEvent) -> None:
        d = auth_event.to_dict()
        assert self.REQUIRED_KEYS.issubset(d.keys())

    def test_success_is_bool(self, auth_event: AuthEvent) -> None:
        assert isinstance(auth_event.to_dict()["success"], bool)

    @pytest.mark.parametrize("action", ["login", "logout", "register"])
    def test_valid_actions(self, action: str) -> None:
        ev = AuthEvent(
            user_id="u1", action=action, success=True,
            method="email", platform="web",
        )
        assert ev.to_dict()["action"] == action


# ── Location ──────────────────────────────────────────────────────────────────


class TestLocation:
    def test_location_fields(self, location: Location) -> None:
        assert location.country == "USA"
        assert location.city == "New York"
        assert -90 <= location.latitude <= 90
        assert -180 <= location.longitude <= 180


# ── Song / User dataclasses ───────────────────────────────────────────────────


class TestSongUser:
    def test_song_fields(self) -> None:
        song = Song(
            song_id=str(uuid.uuid4()),
            song_name="Hey Jude",
            artist_name="The Beatles",
            album_name="The Beatles Again",
            genre="Rock",
            release_year=1968,
            duration_ms=431_000,
        )
        assert song.genre == "Rock"
        assert song.release_year == 1968

    def test_user_subscription_levels(self) -> None:
        for level in ("free", "premium"):
            user = User(
                user_id=str(uuid.uuid4()),
                username="john",
                email="john@example.com",
                age=25,
                gender="M",
                country="USA",
                city="NY",
                subscription_level=level,
                registration_date="2023-01-01",
            )
            assert user.subscription_level == level
