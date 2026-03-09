import json
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, Field, ValidationError


CONFIG_DIR = Path(__file__).resolve().parents[2] / "configs"


class RoutingRule(BaseModel):
    rule_id: str = Field(..., description="Unique identifier for the rule")
    match_type: str = Field(
        ...,
        description="Type of match: keyword | field_equals | field_missing | always",
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
    category: Optional[str] = Field(
        None,
        description="Optional category to assign when this rule matches (client policy, may be None for scaffolding)",
    )
    risk_level: str = Field(
        "low",
        description="Risk level emitted when rule matches: low | medium | high",
    )


class RoutingConfig(BaseModel):
    """
    Minimal routing config schema (Phase 1 scaffolding).

    - routes: allowed route names (deterministic outputs)
    - categories: placeholder for client operational taxonomy (5–7); may be empty until client provides
    - rules: deterministic match rules (policy-as-data)
    """

    version: str = Field(..., description="Config version string")
    routes: List[str] = Field(..., description="Allowed route names")
    categories: List[str] = Field(default_factory=list, description="Allowed category names (may be empty initially)")
    rules: List[RoutingRule] = Field(default_factory=list)

    def validate_internal_consistency(self) -> None:
        route_set = set(self.routes)
        category_set = set(self.categories)
        allowed_risk = {"low", "medium", "high"}

        for rule in self.rules:
            if rule.route not in route_set:
                raise ValueError(
                    f"Rule '{rule.rule_id}' references undefined route '{rule.route}'"
                )
            if rule.category is not None and rule.category not in category_set:
                raise ValueError(
                    f"Rule '{rule.rule_id}' references undefined category '{rule.category}'"
                )
            if rule.risk_level not in allowed_risk:
                raise ValueError(
                    f"Rule '{rule.rule_id}' has invalid risk_level '{rule.risk_level}'"
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