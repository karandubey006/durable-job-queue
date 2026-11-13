"""Timing settings, read from the environment (D15)."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    lease_seconds: float = 30.0
    heartbeat_seconds: float = 10.0  # lease / 3: two beats can be missed safely
    reaper_interval: float = 5.0

    @classmethod
    def from_env(cls) -> "Settings":
        lease = float(os.environ.get("JOBQ_LEASE_SECONDS", cls.lease_seconds))
        return cls(
            lease_seconds=lease,
            heartbeat_seconds=float(os.environ.get("JOBQ_HEARTBEAT_SECONDS", lease / 3)),
            reaper_interval=float(os.environ.get("JOBQ_REAPER_INTERVAL", cls.reaper_interval)),
        )
