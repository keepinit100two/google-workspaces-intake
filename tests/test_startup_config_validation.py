import pytest

import app.main as main


def test_startup_config_validation_failure_raises(monkeypatch):
    def boom():
        raise RuntimeError("bad config")

    monkeypatch.setattr(main, "load_routing_config", boom)

    with pytest.raises(RuntimeError):
        main._startup_validate_configs()