import json
from pathlib import Path

import pytest

from app.core.config_loader import load_routing_config


def test_load_routing_config_valid(tmp_path: Path):
    p = tmp_path / "routing.json"
    p.write_text(
        json.dumps(
            {
                "version": "1.0",
                "routes": ["A", "B"],
                "rules": [{"rule_id": "r1", "match_type": "always", "field": None, "value": None, "route": "A"}],
            }
        ),
        encoding="utf-8",
    )

    cfg = load_routing_config(path=p)
    assert cfg.version == "1.0"
    assert "A" in cfg.routes
    assert cfg.rules[0].rule_id == "r1"


def test_load_routing_config_rejects_unknown_route(tmp_path: Path):
    p = tmp_path / "routing.json"
    p.write_text(
        json.dumps(
            {
                "version": "1.0",
                "routes": ["A"],
                "rules": [{"rule_id": "r1", "match_type": "always", "field": None, "value": None, "route": "B"}],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(Exception):
        load_routing_config(path=p)