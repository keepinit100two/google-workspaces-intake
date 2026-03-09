from app.services.llm_adapter import OpenAIClassifier


class _FakeResponse:
    def __init__(self, output_text: str):
        self.output_text = output_text


class _FakeResponsesAPI:
    def __init__(self, outputs):
        self.outputs = outputs
        self.calls = []

    def create(self, model, input):
        self.calls.append({"model": model, "input": input})
        return _FakeResponse(self.outputs[len(self.calls) - 1])


class _FakeClient:
    def __init__(self, outputs):
        self.responses = _FakeResponsesAPI(outputs)


def test_llm_adapter_accepts_valid_json_first_try():
    fake_client = _FakeClient(
        [
            '{"schema_version":"1.0","category":"support","confidence":0.92,"reason":"Operational support issue","keywords":["support"]}'
        ]
    )
    adapter = OpenAIClassifier(model="gpt-test", client=fake_client)

    result = adapter.classify(
        text="The VPN is down for multiple users.",
        allowed_categories=["support", "billing"],
    )

    assert result.accepted is True
    assert result.final_status == "accepted"
    assert result.classification is not None
    assert result.classification.category == "support"
    assert result.classification.confidence == 0.92
    assert len(result.attempts) == 1
    assert result.attempts[0].status == "ok"


def test_llm_adapter_retries_after_invalid_json_then_accepts():
    fake_client = _FakeClient(
        [
            'not json at all',
            '{"schema_version":"1.0","category":"billing","confidence":0.88,"reason":"Billing-related request","keywords":["invoice"]}',
        ]
    )
    adapter = OpenAIClassifier(model="gpt-test", client=fake_client)

    result = adapter.classify(
        text="Can you help with my invoice?",
        allowed_categories=["support", "billing"],
    )

    assert result.accepted is True
    assert result.classification is not None
    assert result.classification.category == "billing"
    assert len(result.attempts) == 2
    assert result.attempts[0].status == "invalid_json"
    assert result.attempts[1].status == "ok"


def test_llm_adapter_rejects_unknown_category():
    fake_client = _FakeClient(
        [
            '{"schema_version":"1.0","category":"sales","confidence":0.91,"reason":"Looks like sales","keywords":["demo"]}',
            '{"schema_version":"1.0","category":"sales","confidence":0.90,"reason":"Still sales","keywords":["demo"]}',
            '{"schema_version":"1.0","category":"sales","confidence":0.89,"reason":"Still sales","keywords":["demo"]}',
        ]
    )
    adapter = OpenAIClassifier(model="gpt-test", client=fake_client)

    result = adapter.classify(
        text="I want a product demo.",
        allowed_categories=["support", "billing"],
    )

    assert result.accepted is False
    assert result.final_status == "rejected"
    assert result.reject_reason == "invalid_schema"
    assert len(result.attempts) == 3
    assert all(a.status == "invalid_schema" for a in result.attempts)


def test_llm_adapter_rejects_when_allowed_categories_missing():
    fake_client = _FakeClient([])
    adapter = OpenAIClassifier(model="gpt-test", client=fake_client)

    result = adapter.classify(
        text="Some email body",
        allowed_categories=[],
    )

    assert result.accepted is False
    assert result.final_status == "rejected"
    assert result.reject_reason == "missing_allowed_categories"
    assert result.attempts == []