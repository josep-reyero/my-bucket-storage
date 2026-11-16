"""Shared fixtures: a fresh store per test, a bucket in it, and a controllable clock."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from mybucket import open_store

SRC = Path(__file__).resolve().parents[1] / "src"


class FakeClock:
    """Unix-ms clock that only moves when told to."""

    def __init__(self, start_ms: int = 1_700_000_000_000) -> None:
        self.now = start_ms

    def __call__(self) -> int:
        return self.now

    def advance(self, *, days: float = 0, seconds: float = 0) -> None:
        self.now += int(days * 86_400_000 + seconds * 1000)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def store_path(tmp_path: Path) -> Path:
    return tmp_path / "store"


@pytest.fixture
def store(store_path: Path, clock: FakeClock):
    s = open_store(store_path, clock=clock)
    yield s
    s.close()


@pytest.fixture
def bucket(store):
    return store.bucket("test-bucket", create=True)


def run_python(code: str, timeout: float = 60.0, check: bool = True) -> subprocess.CompletedProcess:
    """Run ``code`` in a fresh interpreter with mybucket importable."""
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC) + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )
    if check and proc.returncode != 0:
        raise AssertionError(f"child failed ({proc.returncode}):\n{proc.stdout}\n{proc.stderr}")
    return proc


def popen_python(code: str, **kwargs) -> subprocess.Popen:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC) + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.Popen([sys.executable, "-c", textwrap.dedent(code)], env=env, text=True, **kwargs)
