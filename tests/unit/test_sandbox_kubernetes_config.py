"""Kubernetes config matrix. All cases are unexecuted delivery material."""

from __future__ import annotations

from dataclasses import fields
from typing import Any

import pytest

from agent_runtime.core.config import RuntimeConfig, validate_runtime_sandbox
from agent_runtime.core.sandbox import config as module
from agent_runtime.core.sandbox.config import (
    SandboxConfig,
    apply_sandbox_env,
    apply_sandbox_table,
    validate_sandbox_config,
)


def configured() -> SandboxConfig:
    config = SandboxConfig(backend="kubernetes", deployment_scope="unit-scope")
    config.kubernetes.image = "example.test/worker@sha256:" + "a" * 64
    config.kubernetes.ownership_key_file = "/run/ownership/key"
    config.kubernetes.network_policy_verified = True
    return config


def test_valid_config_does_not_load_kubeconfig(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module.importlib.util, "find_spec", lambda _: object())
    validate_sandbox_config(configured())


def test_local_does_not_inspect_optional_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(name: str) -> None:
        raise AssertionError("Local inspected Kubernetes dependency")

    monkeypatch.setattr(module.importlib.util, "find_spec", forbidden)
    validate_sandbox_config(SandboxConfig())


def test_missing_optional_extra_fails_clearly(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module.importlib.util, "find_spec", lambda _: None)
    with pytest.raises(SystemExit, match="optional extra"):
        validate_sandbox_config(configured())


@pytest.mark.parametrize(
    ("section", "name", "value"),
    [
        ("sandbox", "deployment_scope", "Bad Scope"),
        ("sandbox", "idle_timeout_s", True),
        ("sandbox", "queue_timeout_s", 0.0),
        ("sandbox", "command_timeout_s", 121.0),
        ("sandbox", "cleanup_margin_s", float("nan")),
        ("kubernetes", "namespace", "../default"),
        ("kubernetes", "service_port", 65536),
        ("kubernetes", "image", "worker:latest"),
        ("kubernetes", "cpu_request", "2"),
        ("kubernetes", "memory_limit", "64Mi"),
        ("kubernetes", "tmp_size_limit", "2Gi"),
        ("kubernetes", "cpu_limit", "0.0001"),
        ("kubernetes", "memory_request", "-1Gi"),
        ("kubernetes", "network_policy_verified", False),
        ("kubernetes", "ownership_key_file", "relative/key"),
        ("kubernetes", "runtime_class_name", "../runc"),
        ("kubernetes", "journal_max_bytes", 1024),
    ],
)
def test_invalid_fields_fail_fast(section: str, name: str, value: Any) -> None:
    config = configured()
    setattr(config if section == "sandbox" else config.kubernetes, name, value)
    with pytest.raises(SystemExit):
        validate_sandbox_config(config, dependencies=False)


def test_every_public_field_has_an_environment_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    expected = configured()
    for target, prefix in (
        (expected, "AGENTRT_SANDBOX_"),
        (expected.kubernetes, "AGENTRT_SANDBOX_KUBERNETES_"),
    ):
        for item in fields(target):
            if item.name != "kubernetes":
                monkeypatch.setenv(prefix + item.name.upper(), str(getattr(target, item.name)))
    actual = SandboxConfig()
    apply_sandbox_env(actual)
    assert actual == expected


def test_unknown_raw_pod_spec_is_rejected() -> None:
    with pytest.raises(SystemExit, match="Unknown"):
        apply_sandbox_table(SandboxConfig(), {"kubernetes": {"pod_spec": {}}})


@pytest.mark.parametrize("explicit", [False, True])
def test_durable_rejection_precedes_dependency_and_network_setup(explicit: bool) -> None:
    config = RuntimeConfig(sandbox=configured())
    if not explicit:
        config.agent.engine = "graph"
        config.graph.checkpoint_backend = "sqlite"
    with pytest.raises(SystemExit, match="require M3"):
        validate_runtime_sandbox(config, durable=explicit)
