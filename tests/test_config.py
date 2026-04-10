import os

from app.config.settings import load_config


def test_load_config_defaults():
    config = load_config()
    assert config.base_dir.exists()
    assert config.db_path is not None
    assert config.log_dir is not None


def test_load_config_env_override(monkeypatch):
    monkeypatch.setenv("DATABASE_PATH", ":memory:")
    config = load_config()
    assert str(config.db_path) == ":memory:"
