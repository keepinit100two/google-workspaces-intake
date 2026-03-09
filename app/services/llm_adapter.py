import json
import os
from dataclasses import dataclass
from typing import List, Optional, Protocol

from app.domain.schemas import AiAttempt, AiClassification


class LLMClassifier(Protocol):
    """
    Vendor-agnostic classifier interface.

    Router/Decide logic should depend on this contract, not on a specific vendor SDK.
    """

    def classify(self, text: str, allowed_categories: List[str]) -> "LLMClassificationOutcome":
        ...


@dataclass(frozen=True)
class LLMClassificationOutcome:
    """
    Deterministic result of an LLM classification attempt sequence.
    """

    accepted: bool
    classification: Optional[AiClassification]
    attempts: List[AiAttempt]
    final_status: str  # accepted | rejected | not_used
    reject_reason: Optional[str]


class OpenAIClassifier:
    """
    OpenAI-backed implementation using the Responses API.

    Design goals:
    - strict JSON-only output
    - bounded retries
    - deterministic reject/fallback posture
    - no routing decisions here; only structured enrichment
    """

    def __init__(
        self,
        model: Optional[str] = None,
        max_attempts: int = 3,
        client=None,
    ):
        self.model = model or os.getenv("OPENAI_MODEL", "gpt-5.4")
        self.max_attempts = max_attempts

        if client is not None:
            self.client = client
        else:
            from openai import OpenAI
            self.client = OpenAI()

    def _build_prompt(self, text: str, allowed_categories: List[str], attempt: int) -> str:
        categories_json = json.dumps(allowed_categories)

        base = (
            "Classify the following inbound operational email text.\n"
            "Return ONLY valid JSON.\n"
            "No markdown. No prose. No code fences.\n"
            "Schema:\n"
            '{'
            '"schema_version":"1.0",'
            '"category":"<one of allowed categories>",'
            '"confidence":<float 0.0 to 1.0>,'
            '"reason":"<optional short reason, max 240 chars>",'
            '"keywords":["<optional keyword>", "<optional keyword>"]'
            '}\n'
            f"Allowed categories: {categories_json}\n"
            f"Text: {text}\n"
        )

        if attempt == 1:
            return base

        if attempt == 2:
            return (
                base
                + "\nFORMAT GUARD: Your entire response must be a single valid JSON object matching the schema exactly."
            )

        return (
            base
            + "\nFINAL FORMAT GUARD: Respond with ONLY a valid JSON object. "
            + "If unsure, still choose the closest allowed category and provide a confidence score."
        )

    def _validate_allowed_category(self, parsed: AiClassification, allowed_categories: List[str]) -> None:
        if parsed.category not in allowed_categories:
            raise ValueError(f"category '{parsed.category}' is not in allowed categories")

    def classify(self, text: str, allowed_categories: List[str]) -> LLMClassificationOutcome:
        if not allowed_categories:
            return LLMClassificationOutcome(
                accepted=False,
                classification=None,
                attempts=[],
                final_status="rejected",
                reject_reason="missing_allowed_categories",
            )

        attempts: List[AiAttempt] = []

        for attempt_num in range(1, self.max_attempts + 1):
            prompt = self._build_prompt(text=text, allowed_categories=allowed_categories, attempt=attempt_num)

            try:
                response = self.client.responses.create(
                    model=self.model,
                    input=prompt,
                )

                raw_text = response.output_text
                parsed_json = json.loads(raw_text)
                parsed = AiClassification.model_validate(parsed_json)
                self._validate_allowed_category(parsed, allowed_categories)

                attempts.append(
                    AiAttempt(
                        attempt=attempt_num,
                        status="ok",
                        latency_ms=None,
                        error_detail=None,
                    )
                )

                return LLMClassificationOutcome(
                    accepted=True,
                    classification=parsed,
                    attempts=attempts,
                    final_status="accepted",
                    reject_reason=None,
                )

            except json.JSONDecodeError as e:
                attempts.append(
                    AiAttempt(
                        attempt=attempt_num,
                        status="invalid_json",
                        latency_ms=None,
                        error_detail=str(e)[:240],
                    )
                )
            except ValueError as e:
                attempts.append(
                    AiAttempt(
                        attempt=attempt_num,
                        status="invalid_schema",
                        latency_ms=None,
                        error_detail=str(e)[:240],
                    )
                )
            except Exception as e:
                attempts.append(
                    AiAttempt(
                        attempt=attempt_num,
                        status="other",
                        latency_ms=None,
                        error_detail=str(e)[:240],
                    )
                )

        last_status = attempts[-1].status if attempts else "other"
        reject_reason_map = {
            "invalid_json": "invalid_json",
            "invalid_schema": "invalid_schema",
            "other": "other",
        }

        return LLMClassificationOutcome(
            accepted=False,
            classification=None,
            attempts=attempts,
            final_status="rejected",
            reject_reason=reject_reason_map.get(last_status, "other"),
        )


def get_default_llm_classifier():
    """
    Optional factory:
    - returns OpenAIClassifier when OPENAI_API_KEY is present
    - otherwise returns None

    This keeps the demo realistic without making LLM availability a startup hard dependency.
    """
    if os.getenv("OPENAI_API_KEY"):
        return OpenAIClassifier()
    return None