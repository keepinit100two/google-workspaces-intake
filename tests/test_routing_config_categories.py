import json
from pathlib import Path

import pytest

from app.core.config_loader import load_routing_config


def test_routing_config_accepts_valid_categories(tmp_path: Path):
    p = tmp_path / "routing.json"
    p.write_text(
        json.dumps(
            {
                "version": "1.0",
                "routes": ["NEEDS_REVIEW", "CREATE_DRAFT_TICKET"],
                "categories": ["support", "billing"],
                "rules": [
                    {
                        "rule_id": "r1",
                        "match_type": "always",
                        "field": None,
                        "value": None,
                        "route": "CREATE_DRAFT_TICKET",
                        "category": "support",
                        "risk_level": "low"
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    cfg = load_routing_config(path=p)
    assert cfg.categories == ["support", "billing"]
    assert cfg.rules[0].category == "support"


def test_routing_config_rejects_unknown_category_reference(tmp_path: Path):
    p = tmp_path / "routing.json"
    p.write_text(
        json.dumps(
            {
                "version": "1.0",
                "routes": ["NEEDS_REVIEW", "CREATE_DRAFT_TICKET"],
                "categories": ["support", "billing"],
                "rules": [
                    {
                        "rule_id": "r1",
                        "match_type": "always",
                        "field": None,
                        "value": None,
                        "route": "CREATE_DRAFT_TICKET",
                        "category": "security",
                        "risk_level": "low"
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(Exception):
        load_routing_config(path=p)