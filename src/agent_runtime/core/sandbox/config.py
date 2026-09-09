"""Strict, inert configuration. Importing Local never loads a Kubernetes client."""

from __future__ import annotations

import importlib.util
import math
import os
import re
from dataclasses import dataclass, field, fields
from decimal import Decimal
from typing import Any, Literal

_IMAGE_COMPONENT = r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*"
_IMAGE_REFERENCE = (
    r"(?:[a-z0-9]+(?:[.-][a-z0-9]+)*(?::[0-9]{1,5})?/)?"
    + _IMAGE_COMPONENT
    + r"(?:/"
    + _IMAGE_COMPONENT
    + r")*"
    + r"(?::[A-Za-z0-9_][A-Za-z0-9_.-]{0,127})?@sha256:[0-9a-f]{64}"
)


@dataclass
class KubernetesConfig:
    namespace: str = "kitagent-sandboxes"
    image: str = ""  # Required digest of the image built from sandbox_server/Dockerfile.
    service_port: int = 8080
    ready_timeout_s: float = 120.0
    runtime_class_name: str = ""
    cpu_request: str = "100m"
    cpu_limit: str = "1"
    memory_request: str = "128Mi"
    memory_limit: str = "512Mi"
    ephemeral_storage_request: str = "256Mi"
    ephemeral_storage_limit: str = "1Gi"
    workspace_size_limit: str = "512Mi"
    tmp_size_limit: str = "128Mi"
    core_namespace: str = "kitagent-core"
    # Explicit operator attestation after validating the installed CNI. Default
    # cannot enable execution merely because NetworkPolicy objects exist.
    network_policy_verified: bool = False
    ownership_key_file: str = ""  # Stable Core-only HMAC key, >=32 bytes.
    journal_max_entries: int = 1024
    journal_max_bytes: int = 16777216


@dataclass
class SandboxConfig:
    backend: Literal["local", "kubernetes"] = "local"
    deployment_scope: str = ""
    create_on: Literal["first_tool"] = "first_tool"
    idle_timeout_s: float = 900.0
    queue_timeout_s: float = 120.0
    command_timeout_s: float = 120.0
    cleanup_margin_s: float = 5.0
    reconcile_grace_s: float = 120.0
    kubernetes: KubernetesConfig = field(default_factory=KubernetesConfig)


def _error(message: str) -> None:
    raise SystemExit(f"Config error: {message}")


def _assign(target: Any, key: str, value: object, source: str) -> None:
    previous = getattr(type(target)(), key)
    if isinstance(previous, bool):
        valid = isinstance(value, bool)
    elif isinstance(previous, int):
        valid = isinstance(value, int) and not isinstance(value, bool)
    elif isinstance(previous, float):
        valid = isinstance(value, (float, int)) and not isinstance(value, bool)
    else:
        valid = isinstance(value, str)
    if not valid:
        _error(f"{source} has an invalid type")
    if isinstance(previous, float) and isinstance(value, (int, float)):
        value = float(value)
    setattr(target, key, value)


def apply_sandbox_table(config: SandboxConfig, table: object) -> None:
    if not isinstance(table, dict):
        _error("[sandbox] must be a table")
    assert isinstance(table, dict)
    known = {item.name for item in fields(config)}
    if set(table) - known:
        _error(f"Unknown [sandbox] keys: {', '.join(sorted(set(table) - known))}")
    for key, value in table.items():
        if key == "kubernetes":
            if not isinstance(value, dict):
                _error("[sandbox.kubernetes] must be a table")
            assert isinstance(value, dict)
            allowed = {item.name for item in fields(config.kubernetes)}
            if set(value) - allowed:
                _error(
                    "Unknown [sandbox.kubernetes] keys: " + ", ".join(sorted(set(value) - allowed))
                )
            for name, item in value.items():
                _assign(config.kubernetes, name, item, f"sandbox.kubernetes.{name}")
        else:
            _assign(config, key, value, f"sandbox.{key}")


def apply_sandbox_env(config: SandboxConfig) -> None:
    for target, prefix in (
        (config, "AGENTRT_SANDBOX_"),
        (config.kubernetes, "AGENTRT_SANDBOX_KUBERNETES_"),
    ):
        for item in fields(target):
            if item.name == "kubernetes":
                continue
            env = prefix + item.name.upper()
            raw = os.environ.get(env)
            if raw is None:
                continue
            default = getattr(type(target)(), item.name)
            try:
                value: object = raw
                if isinstance(default, bool):
                    if raw.lower() not in ("true", "false", "1", "0"):
                        raise ValueError
                    value = raw.lower() in ("true", "1")
                elif isinstance(default, int):
                    value = int(raw)
                elif isinstance(default, float):
                    value = float(raw)
                _assign(target, item.name, value, env)
            except ValueError:
                _error(f"{env} has an invalid value")


def _dns(value: str, field_name: str, *, subdomain: bool = False) -> None:
    parts = value.split(".") if subdomain else [value]
    if len(value) > (253 if subdomain else 63) or not all(
        re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", part) for part in parts
    ):
        _error(f"{field_name} must be a Kubernetes DNS {'subdomain' if subdomain else 'label'}")


def quantity(value: str, *, cpu: bool = False) -> Decimal:
    """Intentionally support a strict subset of Kubernetes resource quantities."""
    pattern = r"([0-9]+(?:\.[0-9]{1,3})?)(m?)" if cpu else r"([0-9]+)(Ki|Mi|Gi|Ti)?"
    match = re.fullmatch(pattern, value)
    if match is None:
        _error(f"unsupported resource quantity {value!r}")
    assert match is not None
    number = Decimal(match[1])
    suffix = match[2] or ""
    multipliers: dict[str, Decimal] = {
        "": Decimal(1),
        "m": Decimal("0.001"),
        "Ki": Decimal(1024),
        "Mi": Decimal(1024**2),
        "Gi": Decimal(1024**3),
        "Ti": Decimal(1024**4),
    }
    multiplier = multipliers[suffix]
    number *= multiplier
    if number <= 0:
        _error("resource quantities must be positive")
    if cpu and number * 1000 != (number * 1000).to_integral_value():
        _error("CPU precision must be at least 1m")
    return number


def validate_sandbox_config(config: SandboxConfig, *, dependencies: bool = True) -> None:
    # Also validate directly constructed dataclasses and otherwise-unused Local
    # settings; malformed policy fields must never appear to have taken effect.
    if not isinstance(config, SandboxConfig) or not isinstance(config.kubernetes, KubernetesConfig):
        _error("sandbox and sandbox.kubernetes require typed configuration models")
    defaults = SandboxConfig()
    for target, default in ((config, defaults), (config.kubernetes, defaults.kubernetes)):
        for item in fields(default):
            if item.name != "kubernetes":
                _assign(default, item.name, getattr(target, item.name), f"sandbox.{item.name}")
    if config.backend not in ("local", "kubernetes"):
        _error("sandbox.backend must be 'local' or 'kubernetes'")
    if config.create_on != "first_tool":
        _error("sandbox.create_on must be 'first_tool'")
    if config.command_timeout_s > 120:
        _error("sandbox.command_timeout_s must not exceed 120 seconds")
    for name in (
        "idle_timeout_s",
        "queue_timeout_s",
        "command_timeout_s",
        "cleanup_margin_s",
        "reconcile_grace_s",
    ):
        value = getattr(config, name)
        if not math.isfinite(value) or not 0 < value <= 86400:
            _error(f"sandbox.{name} must be finite and in (0, 86400]")
    k = config.kubernetes
    if not 0 < k.service_port <= 65535:
        _error("sandbox.kubernetes.service_port must be in [1, 65535]")
    if not math.isfinite(k.ready_timeout_s) or not 0 < k.ready_timeout_s <= 3600:
        _error("sandbox.kubernetes.ready_timeout_s must be in (0, 3600]")
    if (
        not 1 <= k.journal_max_entries <= 100000
        or not 8 * 2**20 <= k.journal_max_bytes <= 64 * 2**20
    ):
        _error("journal limits must be 1..100000 entries and 8MiB..64MiB")
    _dns(k.namespace, "sandbox.kubernetes.namespace")
    _dns(k.core_namespace, "sandbox.kubernetes.core_namespace")
    if k.namespace == k.core_namespace:
        _error("Core and Sandbox must use separate namespaces")
    if k.runtime_class_name:
        _dns(k.runtime_class_name, "runtime_class_name", subdomain=True)
    for request, limit, cpu in (
        (k.cpu_request, k.cpu_limit, True),
        (k.memory_request, k.memory_limit, False),
        (k.ephemeral_storage_request, k.ephemeral_storage_limit, False),
    ):
        if quantity(request, cpu=cpu) > quantity(limit, cpu=cpu):
            _error("resource requests must not exceed limits")
    writable_size = quantity(k.workspace_size_limit) + quantity(k.tmp_size_limit)
    if writable_size > quantity(k.ephemeral_storage_limit):
        _error("workspace + tmp must fit ephemeral_storage_limit")
    if config.deployment_scope:
        _dns(config.deployment_scope, "sandbox.deployment_scope")
    if k.image and (len(k.image) > 512 or not re.fullmatch(_IMAGE_REFERENCE, k.image)):
        _error("sandbox.kubernetes.image must be pinned with @sha256:<64 lowercase hex>")
    registry_port = re.match(r"^[^/]+:([0-9]+)/", k.image)
    if registry_port and not 1 <= int(registry_port[1]) <= 65535:
        _error("sandbox image registry port must be in [1, 65535]")
    if config.backend == "local":
        return
    if not config.deployment_scope or not k.image:
        _error("Kubernetes requires deployment_scope and a fixed image digest")
    if not k.ownership_key_file or not _path_is_absolute(k.ownership_key_file):
        _error("Kubernetes requires an absolute Core-only ownership_key_file")
    if not k.network_policy_verified:
        _error("Kubernetes requires network_policy_verified=true after CNI enforcement validation")
    if dependencies:
        for module in ("kubernetes_asyncio", "aiohttp"):
            if importlib.util.find_spec(module) is None:
                _error(
                    "Kubernetes backend requires the optional extra: "
                    "pip install 'agent-runtime-kit[kubernetes]'"
                )


def _path_is_absolute(value: str) -> bool:
    # The runtime is POSIX; reject Windows drive-relative paths and whitespace.
    return value.startswith("/") and "\x00" not in value and value.strip() == value
