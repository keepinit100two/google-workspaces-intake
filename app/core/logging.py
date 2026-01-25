import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

# Log file path: <repo_root>/logs/events.jsonl
LOG_FILE_PATH = Path(__file__).resolve().parents[2] / "logs" / "events.jsonl"


def get_logger(name: str = "ai-control-plane") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        # Avoid duplicate handlers in reload mode
        return logger

    logger.setLevel(logging.INFO)

    # Console handler (shows in terminal)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(console_handler)

    return logger


def log_event(logger: logging.Logger, event_name: str, fields: Dict[str, Any]) -> None:
    record = {
        "ts": datetime.utcnow().isoformat(),
        "event": event_name,
        **fields,
    }

    line = json.dumps(record, default=str)

    # 1) Emit to terminal (stdout)
    logger.info(line)

    # 2) Persist to JSONL file for ops/reporting
    LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_FILE_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
