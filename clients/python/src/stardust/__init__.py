from __future__ import annotations

from .client import connect

__all__ = [
    "connect",
]

# TODO(embedded): expose an embedded connect() variant when a Python extension
# module is available, e.g. `connect("embedded:/path")` delegating to a
# pybind11-backed `stardust_engine` package.
