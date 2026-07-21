"""Capability adapter around the privileged platform backend.

Keeping the adapter small makes the Streamlit application importable in tests and
allows the platform module to evolve without exposing credentials to the UI.
"""

from __future__ import annotations

import importlib
from typing import Any, Callable


class BackendCapabilityError(RuntimeError):
    pass


def load_backend() -> object:
    return importlib.import_module("services.deploy.console_backend")


class BackendAdapter:
    def __init__(self, backend: object | None = None) -> None:
        self.backend = backend or load_backend()

    def has(self, name: str) -> bool:
        return callable(getattr(self.backend, name, None))

    def call(self, name: str, /, *args: Any, **kwargs: Any) -> Any:
        function: Callable[..., Any] | None = getattr(self.backend, name, None)
        if not callable(function):
            raise BackendCapabilityError(
                f"Platform backend does not provide the '{name}' capability."
            )
        return function(*args, **kwargs)

