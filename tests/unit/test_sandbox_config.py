"""M0 configuration regression cases; added but not executed for this delivery."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from agent_runtime.core.config import RuntimeConfig, get_config, validate_runtime_sandbox


@pytest.fixture
def config_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    for name in tuple(os.environ):
        if name.startswith("AGENTRT_"):
            monkeypatch.delenv(name)
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "config.toml"
    path.write_text("", encoding="utf-8")
    monkeypatch.setenv("AGENTRT_CONFIG", str(path))
    return path


def test_default_local_requires_no_sandbox_section(config_path: Path) -> None:
    assert RuntimeConfig().sandbox.backend == "local"
    assert get_config().sandbox.backend == "local"


def test_explicit_local_toml(config_path: Path) -> None:
    config_path.write_text('[sandbox]\nbackend = "local"\n', encoding="utf-8")
    assert get_config().sandbox.backend == "local"


def test_environment_override_is_applied_before_capability_validation(
    config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path.write_text('[sandbox]\nbackend = "kubernetes"\n', encoding="utf-8")
    monkeypatch.setenv("AGENTRT_SANDBOX_BACKEND", "local")
    assert get_config().sandbox.backend == "local"


def test_system_environment_overrides_dotenv_and_toml(
    config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path.write_text('[sandbox]\nbackend = "local"\n', encoding="utf-8")
    (config_path.parent / ".env").write_text(
        "AGENTRT_SANDBOX_BACKEND=kubernetes\n", encoding="utf-8"
    )
    monkeypatch.setenv("AGENTRT_SANDBOX_BACKEND", "local")
    assert get_config().sandbox.backend == "local"


@pytest.mark.parametrize("source", ["toml", "environment"])
def test_kubernetes_without_required_fields_fails_fast(
    source: str, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if source == "toml":
        config_path.write_text('[sandbox]\nbackend = "kubernetes"\n', encoding="utf-8")
    else:
        monkeypatch.setenv("AGENTRT_SANDBOX_BACKEND", "kubernetes")
    with pytest.raises(SystemExit, match="requires deployment_scope"):
        get_config()


@pytest.mark.parametrize("backend", ["docker", "Local", "", " local "])
@pytest.mark.parametrize("source", ["toml", "environment"])
def test_unknown_backend_is_configuration_error(
    backend: str, source: str, config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if source == "toml":
        config_path.write_text(f'[sandbox]\nbackend = "{backend}"\n', encoding="utf-8")
    else:
        monkeypatch.setenv("AGENTRT_SANDBOX_BACKEND", backend)
    with pytest.raises(SystemExit, match="must be 'local' or 'kubernetes'"):
        get_config()


@pytest.mark.parametrize("value", ["true", "7", "[]", "{}"])
def test_backend_requires_string(value: str, config_path: Path) -> None:
    config_path.write_text(f"[sandbox]\nbackend = {value}\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="sandbox.backend.*invalid type"):
        get_config()


@pytest.mark.parametrize(
    "section",
    [
        "sandbox = 'local'",
        "[sandbox]\nworkspace = '/workspace'",
    ],
)
def test_undelivered_configuration_is_rejected(section: str, config_path: Path) -> None:
    config_path.write_text(section, encoding="utf-8")
    with pytest.raises(SystemExit, match=r"(Unknown \[sandbox\] keys|\[sandbox\] must be a table)"):
        get_config()


def test_programmatic_config_validation_rejects_unsupported_backend() -> None:
    config = RuntimeConfig()
    config.sandbox.backend = "kubernetes"
    with pytest.raises(SystemExit, match="requires deployment_scope"):
        validate_runtime_sandbox(config)
