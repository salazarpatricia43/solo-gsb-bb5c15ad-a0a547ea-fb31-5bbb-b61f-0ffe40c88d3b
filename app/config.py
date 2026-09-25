"""Application configuration read from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # SQLite database file. The parent directory is created on startup.
    db_path: str = os.getenv("SENSOR_DB_PATH", "/data/sensors.db")
    # uvicorn bind settings (used by the container entrypoint)
    host: str = os.getenv("SENSOR_HOST", "0.0.0.0")
    port: int = int(os.getenv("SENSOR_PORT", "8000"))


settings = Settings()
