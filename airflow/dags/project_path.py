"""Ensure repo `src/` is importable inside the Airflow container and local pytest."""

from __future__ import annotations

import sys
from pathlib import Path

_CANDIDATES = [
    Path("/opt/airflow/src"),
    Path(__file__).resolve().parents[2] / "src",
]
for _src in _CANDIDATES:
    if _src.is_dir() and str(_src) not in sys.path:
        sys.path.insert(0, str(_src))
