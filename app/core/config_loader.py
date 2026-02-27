import json
import os
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, ValidationError


CONFIG_DIR = Path(__file__).resolve().parents[2] / "configs"


class RoutingRule(BaseModel):
    rule_id: str = Field(..., description="Unique identifier for the rule")
    match_type: str = Field(
        ...,
        description="Type of match: keyword | field_equals | always",
    )
    field: Optional[str] = Field(
        None,
        description="Field in payload to evaluate (if applicable)",
    )
    value: Optional[str] = Field(
        None,
        description="Value to match against (if applicable)",
    )
    route: str = Field(..., description="Route to emit if rule matches")


class RoutingConfig(BaseModel):
    """
    Minimal routing config schema.

    This is intentionally strict and minimal for Phase 1.
    """

    version: str = Field(..., description="Config version string")
    routes: List[str] = Field(..., description="Allowed route names")
    rules: List[RoutingRule] = Field(default_factory=list)

    def validate_internal_consistency(self) -> None:
        route_set = set(self.routes)
        for rule in self.rules:
            if rule.route not in route_set:
                raise ValueError(
                    f"Rule '{rule.rule_id}' references undefined route '{rule.route}'"
                )


def load_routing_config(path: Optional[Path] = None) -> RoutingConfig:
    """
    Load and validate routing configuration.
    Raises on invalid structure or internal inconsistency.
    """
    config_path = path or CONFIG_DIR / "routing.json"

    if not config_path.exists():
        raise FileNotFoundError(f"Routing config not found: {config_path}")

    raw = json.loads(config_path.read_text(encoding="utf-8"))

    try:
        cfg = RoutingConfig.model_validate(raw)
    except ValidationError as e:
        raise RuntimeError(f"Routing config validation failed: {e}") from e

    cfg.validate_internal_consistency()
    return cfg