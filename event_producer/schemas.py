"""
Data models (schemas) for all event types produced by the fake
music-streaming service.  Each class exposes a ``to_dict()`` method that
returns a JSON-serialisable dictionary.
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional


# ── Helper ──────────────────────────────────────────────────────────────────


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _new_id() -> str:
    return str(uuid.uuid4())


# ── Value objects ────────────────────────────────────────────────────────────


@dataclass
class Location:
    country: str
    city: str
    latitude: float
    longitude: float


@dataclass
class Song:
    song_id: str
    song_name: str
    artist_name: str
    album_name: str
    genre: str
    release_year: int
    duration_ms: int


@dataclass
class User:
    user_id: str
    username: str
    email: str
    age: int
    gender: str
    country: str
    city: str
    subscription_level: str
    registration_date: str


# ── Events ───────────────────────────────────────────────────────────────────


@dataclass
class ListenEvent:
    """Fired every time a user plays a track (or part of a track)."""

    user_id: str
    song_id: str
    song_name: str
    artist_name: str
    album_name: str
    genre: str
    duration_ms: int
    ms_played: int
    platform: str
    location: Location

    event_type: str = field(default="listen", init=False)
    event_id: str = field(default_factory=_new_id)
    timestamp: str = field(default_factory=_now_iso)

    def to_dict(self) -> dict:
        d = asdict(self)
        return d


@dataclass
class PageViewEvent:
    """Fired when a user navigates to a page in the app."""

    user_id: str
    session_id: str
    page: str
    prev_page: Optional[str]
    platform: str

    event_type: str = field(default="page_view", init=False)
    event_id: str = field(default_factory=_new_id)
    timestamp: str = field(default_factory=_now_iso)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AuthEvent:
    """Fired on login / logout / register actions."""

    user_id: str
    action: str          # login | logout | register
    success: bool
    method: str          # email | google | facebook
    platform: str

    event_type: str = field(default="auth", init=False)
    event_id: str = field(default_factory=_new_id)
    timestamp: str = field(default_factory=_now_iso)

    def to_dict(self) -> dict:
        return asdict(self)
