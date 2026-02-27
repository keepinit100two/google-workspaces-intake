import json
from pathlib import Path

from app.core.failure_sink import JsonlFileFailureSink, make_failure


def test_failure_sink_writes_jsonl(tmp_path: Path):
    path = tmp_path / "failures.jsonl"
    sink = JsonlFileFailureSink(path=path)

    record = make_failure(
        stage="ingest",
        error_code="INGEST_REJECTED",
        message="Missing Idempotency-Key",
        context={"source": "api"},
    )

    sink.notify(record)

    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1

    obj = json.loads(lines[0])
    assert obj["stage"] == "ingest"
    assert obj["error_code"] == "INGEST_REJECTED"
    assert obj["message"] == "Missing Idempotency-Key"
    assert obj["context"]["source"] == "api"