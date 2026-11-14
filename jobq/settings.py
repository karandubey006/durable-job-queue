"""Timing settings, read from the environment (D15, D18)."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    lease_seconds: float = 30.0
    heartbeat_seconds: float = 10.0  # lease / 3: two beats can be missed safely
    reaper_interval: float = 5.0
    backoff_base: float = 1.0
    backoff_cap: float = 300.0

    @classmethod
    def from_env(cls) -> "Settings":
        env = lambda name, default: float(os.environ.get(name, default))  # noqa: E731
        lease = env("JOBQ_LEASE_SECONDS", cls.lease_seconds)
        return cls(
            lease_seconds=lease,
            heartbeat_seconds=env("JOBQ_HEARTBEAT_SECONDS", lease / 3),
            reaper_interval=env("JOBQ_REAPER_INTERVAL", cls.reaper_interval),
            backoff_base=env("JOBQ_BACKOFF_BASE", cls.backoff_base),
            backoff_cap=env("JOBQ_BACKOFF_CAP", cls.backoff_cap),
        )
