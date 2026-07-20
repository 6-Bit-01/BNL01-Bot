"""Small standard-library adapters for legacy function-style test modules."""

from __future__ import annotations

from functools import wraps
import inspect
import os
from typing import Any, Callable
from unittest import mock


class MonkeyPatch:
    """Minimal reversible subset used by the converted regression tests."""

    def __init__(self) -> None:
        self._cleanups: list[Callable[[], Any]] = []

    def setenv(self, name: str, value: object) -> None:
        patcher = mock.patch.dict(os.environ, {name: str(value)}, clear=False)
        patcher.start()
        self._cleanups.append(patcher.stop)

    def delenv(self, name: str, raising: bool = True) -> None:
        existed = name in os.environ
        previous = os.environ.get(name)
        if not existed and raising:
            raise KeyError(name)
        os.environ.pop(name, None)

        def restore() -> None:
            if existed:
                os.environ[name] = str(previous)
            else:
                os.environ.pop(name, None)

        self._cleanups.append(restore)

    def setattr(self, target: object, name: str, value: object) -> None:
        patcher = mock.patch.object(target, name, value)
        patcher.start()
        self._cleanups.append(patcher.stop)

    def undo(self) -> None:
        while self._cleanups:
            self._cleanups.pop()()


def install_function_cases(namespace: dict[str, Any], case_class: type) -> None:
    """Attach ``_case_*`` functions as discoverable unittest methods."""

    for name, function in list(namespace.items()):
        if not name.startswith("_case_") or not inspect.isfunction(function):
            continue

        @wraps(function)
        def run_case(self: object, _function: Callable[..., Any] = function) -> None:
            monkeypatch = MonkeyPatch()
            try:
                if inspect.signature(_function).parameters:
                    _function(monkeypatch)
                else:
                    _function()
            finally:
                monkeypatch.undo()

        setattr(case_class, "test_" + name[len("_case_"):], run_case)
