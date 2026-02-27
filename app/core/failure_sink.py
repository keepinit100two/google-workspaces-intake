import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


DEFAULT_FAILURE_PATH = Path(__file__).resolve().parents[2] / "data" / "failures.jsonl"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class FailureRecord:
    """
    Canonical failure record for operator-visible sinks.
    Keep this stable; downstream operators and Sheets schema will map to this.
    """

    timestamp: str
    stage: str  # ingest | normalize | decide | act | observe | improve
    error_code: str
    message: str
    context: Dict[str, Any]


class FailureSink:
    """
    Failure sink interface.

    This enforces the invariant: no silent failures.
    """

    def notify(self, record: FailureRecord) -> None:
        raise NotImplementedError


class JsonlFileFailureSink(FailureSink):
    """
    Operator-visible local failure sink.

    Writes one JSON object per line into data/failures.jsonl.
    """

    def __init__(self, path: Path = DEFAULT_FAILURE_PATH):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def notify(self, record: FailureRecord) -> None:
        payload = {
            "timestamp": record.timestamp,
            "stage": record.stage,
            "error_code": record.error_code,
            "message": record.message,
            "context": record.context,
        }
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, default=str) + "\n")


class LoggingFailureSink(FailureSink):
    """
    Minimal sink that does nothing beyond being callable.
    Useful for unit tests or environments that only want structured logs elsewhere.
    """

    def notify(self, record: FailureRecord) -> None:
        return None


def get_failure_sink() -> FailureSink:
    """
    Factory for selecting failure sink backend.

    FAILURE_SINK_BACKEND:
      - "file" (default) -> JsonlFileFailureSink
      - "log"            -> LoggingFailureSink (no-op sink)
    """
    backend = os.getenv("FAILURE_SINK_BACKEND", "file").strip().lower()
    if backend == "log":
        return LoggingFailureSink()
    return JsonlFileFailureSink()


def make_failure(
    stage: str,
    error_code: str,
    message: str,
    context: Optional[Dict[str, Any]] = None,
) -> FailureRecord:
    return FailureRecord(
        timestamp=_utc_now_iso(),
        stage=stage,
        error_code=error_code,
        message=message,
        context=context or {},
    )