from __future__ import annotations

import signal
from typing import Any

from agent_runtime.core.app import _install_shutdown_handlers


class _ShutdownFlag:
    def __init__(self) -> None:
        self.is_set = False

    def set(self) -> None:
        self.is_set = True


class _UnsupportedSignalLoop:
    def __init__(self) -> None:
        self.threadsafe_calls = 0

    def add_signal_handler(self, sig: signal.Signals, callback: Any) -> None:
        raise NotImplementedError

    def call_soon_threadsafe(self, callback: Any) -> None:
        self.threadsafe_calls += 1
        callback()


def test_shutdown_handlers_fall_back_when_asyncio_signal_handlers_are_unsupported(
    monkeypatch: Any,
) -> None:
    registered: dict[signal.Signals, Any] = {}

    def fake_signal(sig: signal.Signals, handler: Any) -> None:
        registered[sig] = handler

    monkeypatch.setattr(signal, "signal", fake_signal)
    shutdown = _ShutdownFlag()
    loop = _UnsupportedSignalLoop()

    _install_shutdown_handlers(loop, shutdown)  # type: ignore[arg-type]

    assert set(registered) == {signal.SIGINT, signal.SIGTERM}

    registered[signal.SIGINT](signal.SIGINT, None)

    assert shutdown.is_set
    assert loop.threadsafe_calls == 1
