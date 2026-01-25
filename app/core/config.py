import json
from pathlib import Path
from typing import Any, Dict


def load_routing_config() -> Dict[str, Any]:
    config_path = Path(__file__).resolve().parents[2] / "configs" / "routing.json"
    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)
