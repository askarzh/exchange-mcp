"""scripts/_pg.py: the throwaway-Postgres helper must never leak a docker
container when startup fails partway through (port lookup or the readiness
wait) — a raise inside __enter__ means __exit__ never runs, so ThrowawayPostgres
has to clean up after itself on that path. No real docker container is
started here; subprocess.run is patched throughout."""

import shutil
import subprocess

import pytest
from _pg import ThrowawayPostgres


class _FakeCompleted:
    def __init__(self, stdout: str = ""):
        self.stdout = stdout


def test_wait_ready_failure_removes_container(monkeypatch):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        if cmd[:2] == ["docker", "run"]:
            return _FakeCompleted()
        if cmd[:2] == ["docker", "port"]:
            return _FakeCompleted(stdout="0.0.0.0:55432\n")
        if cmd[:2] == ["docker", "rm"]:
            return _FakeCompleted()
        raise AssertionError(f"unexpected subprocess.run call: {cmd}")

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr("_pg.wait_ready",
                        lambda dsn, deadline_s=60.0: (_ for _ in ()).throw(
                            RuntimeError("postgres never became ready")))
    monkeypatch.delenv("EWS_TEST_DATABASE_URL", raising=False)

    monkeypatch.setattr(shutil, "which", lambda _name: "/usr/bin/docker")

    tp = ThrowawayPostgres(name_suffix="unit-test")
    with pytest.raises(RuntimeError, match="postgres never became ready"):
        tp.__enter__()

    rm_calls = [c for c in calls if c[:2] == ["docker", "rm"]]
    assert rm_calls, f"expected a `docker rm -f` cleanup call, calls were: {calls}"
    assert rm_calls[0][:3] == ["docker", "rm", "-f"]
    assert tp._container is None  # __exit__ must not try to remove it again


def test_docker_port_failure_removes_container(monkeypatch):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        if cmd[:2] == ["docker", "run"]:
            return _FakeCompleted()
        if cmd[:2] == ["docker", "port"]:
            raise subprocess.CalledProcessError(1, cmd)
        if cmd[:2] == ["docker", "rm"]:
            return _FakeCompleted()
        raise AssertionError(f"unexpected subprocess.run call: {cmd}")

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.delenv("EWS_TEST_DATABASE_URL", raising=False)

    monkeypatch.setattr(shutil, "which", lambda _name: "/usr/bin/docker")

    tp = ThrowawayPostgres(name_suffix="unit-test-2")
    with pytest.raises(subprocess.CalledProcessError):
        tp.__enter__()

    rm_calls = [c for c in calls if c[:2] == ["docker", "rm"]]
    assert rm_calls, f"expected a `docker rm -f` cleanup call, calls were: {calls}"


def test_exit_is_a_noop_when_dsn_came_from_env(monkeypatch):
    """When EWS_TEST_DATABASE_URL is set, no container is started and __exit__
    must not shell out at all."""
    monkeypatch.setenv("EWS_TEST_DATABASE_URL", "postgresql://x/y")
    calls = []
    monkeypatch.setattr(subprocess, "run", lambda cmd, **kw: calls.append(cmd))

    tp = ThrowawayPostgres()
    assert tp.__enter__() == "postgresql://x/y"
    tp.__exit__(None, None, None)
    assert calls == []
