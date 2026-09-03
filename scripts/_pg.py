"""Throwaway Postgres for tests and boot smoke: honours EWS_TEST_DATABASE_URL,
else runs pgvector/pgvector:pg16 in docker on a random host port and waits
for it to accept connections. Shared by tests/conftest.py and boot_smoke.py
so there is exactly one place that knows how to stand this up."""

from __future__ import annotations

import os
import shutil
import subprocess
import time

import psycopg

PG_IMAGE = "pgvector/pgvector:pg16"


def wait_ready(dsn: str, deadline_s: float = 60.0) -> None:
    end = time.time() + deadline_s
    last: Exception | None = None
    while time.time() < end:
        try:
            with psycopg.connect(dsn, connect_timeout=2):
                return
        except Exception as exc:  # noqa: BLE001 - startup race
            last = exc
            time.sleep(0.5)
    raise RuntimeError(f"postgres never became ready: {last}")


class ThrowawayPostgres:
    """Context manager yielding a DSN: EWS_TEST_DATABASE_URL if set, else a
    docker container torn down on exit."""

    def __init__(self, name_suffix: str = ""):
        self._name_suffix = name_suffix
        self.dsn: str = ""
        self._container: str | None = None

    def __enter__(self) -> str:
        dsn = os.environ.get("EWS_TEST_DATABASE_URL")
        if dsn:
            self.dsn = dsn
            return self.dsn
        if shutil.which("docker") is None:
            raise RuntimeError(
                "no EWS_TEST_DATABASE_URL and no docker — cannot provision Postgres"
            )
        name = f"ewsmcp-pg-{self._name_suffix or os.getpid()}"
        subprocess.run(
            ["docker", "run", "-d", "--rm", "--name", name,
             "-e", "POSTGRES_PASSWORD=test", "-p", "127.0.0.1:0:5432", PG_IMAGE],
            check=True, capture_output=True,
        )
        self._container = name
        port_line = subprocess.run(
            ["docker", "port", name, "5432/tcp"], check=True,
            capture_output=True, text=True,
        ).stdout.strip().splitlines()[0]
        port = port_line.rsplit(":", 1)[1]
        self.dsn = f"postgresql://postgres:test@127.0.0.1:{port}/postgres"
        wait_ready(self.dsn)
        return self.dsn

    def __exit__(self, *exc_info) -> None:
        if self._container:
            subprocess.run(["docker", "rm", "-f", self._container],
                           capture_output=True, check=False)
