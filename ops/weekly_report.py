import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List

LOG_PATH = Path(__file__).resolve().parents[1] / "logs" / "events.jsonl"


def read_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return []
    records: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                # Skip malformed lines rather than crashing ops reporting
                continue
    return records


def main() -> None:
    records = list(read_jsonl(LOG_PATH))

    if not records:
        print("No log records found yet.")
        print(f"Expected log file at: {LOG_PATH}")
        print("Generate some traffic via /docs, then rerun this report.")
        return

    event_counts = Counter()
    route_counts = Counter()
    reason_counts = Counter()

    for r in records:
        event_name = r.get("event")
        if event_name:
            event_counts[event_name] += 1

        if event_name == "decision_created":
            route = r.get("route")
            reason = r.get("reason")
            if route:
                route_counts[str(route)] += 1
            if reason:
                reason_counts[str(reason)] += 1

    print("=== AI Control Plane Ops Report ===")
    print(f"Log file: {LOG_PATH}")
    print(f"Total records: {len(records)}")
    print()

    print("---- Ingest / Decision Event Counts ----")
    for k, v in event_counts.most_common():
        print(f"{k}: {v}")
    print()

    if route_counts:
        print("---- Decision Routes ----")
        for k, v in route_counts.most_common():
            print(f"{k}: {v}")
        print()
    else:
        print("---- Decision Routes ----")
        print("No decision_created records found yet.")
        print()

    if reason_counts:
        print("---- Top Decision Reasons ----")
        for k, v in reason_counts.most_common(10):
            print(f"{k}: {v}")
        print()


if __name__ == "__main__":
    main()
