import json
from pathlib import Path
from typing import Dict, List, Optional

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
        description="Optional category to assign when this rule matches",
    )
    risk_level: str = Field(
        "low",
        description="Risk level emitted when rule matches: low | medium | high",
    )


class RoutingConfig(BaseModel):
    version: str = Field(..., description="Config version string")
    routes: List[str] = Field(..., description="Allowed route names")
    categories: List[str] = Field(default_factory=list, description="Allowed category names")
    category_routes: Dict[str, str] = Field(
        default_factory=dict,
        description="Category-to-route mapping used for accepted LLM classifications",
    )
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

        for category, route in self.category_routes.items():
            if category not in category_set:
                raise ValueError(
                    f"category_routes references undefined category '{category}'"
                )
            if route not in route_set:
                raise ValueError(
                    f"category_routes for '{category}' references undefined route '{route}'"
                )


class ConfidenceThresholdConfig(BaseModel):
    version: str = Field(..., description="Config version string")
    auto_route_threshold: float = Field(..., ge=0.0, le=1.0)
    review_threshold: float = Field(..., ge=0.0, le=1.0)

    def validate_internal_consistency(self) -> None:
        if self.auto_route_threshold < self.review_threshold:
            raise ValueError(
                "auto_route_threshold must be greater than or equal to review_threshold"
            )


def load_routing_config(path: Optional[Path] = None) -> RoutingConfig:
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


def load_confidence_threshold_config(path: Optional[Path] = None) -> ConfidenceThresholdConfig:
    config_path = path or CONFIG_DIR / "confidence_thresholds.json"

    if not config_path.exists():
        raise FileNotFoundError(f"Confidence threshold config not found: {config_path}")

    raw = json.loads(config_path.read_text(encoding="utf-8"))

    try:
        cfg = ConfidenceThresholdConfig.model_validate(raw)
    except ValidationError as e:
        raise RuntimeError(f"Confidence threshold config validation failed: {e}") from e

    cfg.validate_internal_consistency()
    return cfg