"""Presentation layer for the routing deployment console.

The package deliberately contains no SFTP, SSH, subprocess, or database client.
All privileged work is delegated to ``services.deploy.console_backend``.
"""

from .app import render_app

__all__ = ["render_app"]
