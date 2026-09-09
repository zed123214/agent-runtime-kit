"""Closed set of Pod/Service fields: no user-provided PodSpec, env or host mounts."""

from __future__ import annotations

import copy
import re
from decimal import Decimal
from typing import Any

from agent_runtime.core.sandbox.config import SandboxConfig

POLICY_VERSION = "sandbox-deny-v1"
OWNER = "agent-runtime-kit"
PREFIX = "sandbox.agentrt.dev/"


def _quantity_value(value: str) -> Decimal:
    """Compare API-canonical quantities (1000m == 1; 1024Mi == 1Gi)."""
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([A-Za-z]*|[eE][+-]?[0-9]+)", value)
    if match is None or len(value) > 64:
        raise ValueError("invalid API resource quantity")
    number, suffix = Decimal(match[1]), match[2]
    if suffix.startswith(("e", "E")) and suffix[1:].lstrip("+-").isdigit():
        return number * Decimal(10) ** int(suffix[1:])
    scales = {
        "": Decimal(1),
        "n": Decimal("1e-9"),
        "u": Decimal("1e-6"),
        "m": Decimal("1e-3"),
        "k": Decimal("1e3"),
        "K": Decimal("1e3"),
    }
    for power, unit in enumerate("MGTPE", start=2):
        scales[unit] = Decimal(1000) ** power
    for power, unit in enumerate(("Ki", "Mi", "Gi", "Ti", "Pi", "Ei"), start=1):
        scales[unit] = Decimal(1024) ** power
    if suffix not in scales:
        raise ValueError("invalid API resource suffix")
    return number * scales[suffix]


def comparable_pod_spec(spec: dict[str, Any]) -> dict[str, Any]:
    """Normalize only documented API defaults and quantity representation.

    Kubernetes omits false scalar host* fields when serializing. Treating those
    omissions as conflicts would reject every ordinary Pod. Mandatory controls
    such as automountServiceAccountToken=false are deliberately NOT defaulted.
    """
    result = copy.deepcopy(spec)
    for key in ("hostNetwork", "hostPID", "hostIPC", "shareProcessNamespace"):
        result.setdefault(key, False)
    for container in result.get("containers", []):
        container.setdefault("securityContext", {}).setdefault("privileged", False)
        for quantities in container.get("resources", {}).values():
            if isinstance(quantities, dict):
                for key, value in quantities.items():
                    quantities[key] = _quantity_value(str(value))
    for volume in result.get("volumes", []):
        empty_dir = volume.get("emptyDir", {})
        if "sizeLimit" in empty_dir:
            empty_dir["sizeLimit"] = _quantity_value(str(empty_dir["sizeLimit"]))
    return result


def network_policies(config: SandboxConfig) -> dict[str, dict[str, Any]]:
    return {
        "sandbox-default-deny": {
            "podSelector": {},
            "policyTypes": ["Ingress", "Egress"],
            "ingress": [],
            "egress": [],
        },
        "sandbox-allow-core": {
            "podSelector": {"matchLabels": {PREFIX + "owner": OWNER}},
            "policyTypes": ["Ingress"],
            "ingress": [
                {
                    "from": [
                        {
                            "namespaceSelector": {
                                "matchLabels": {
                                    "kubernetes.io/metadata.name": config.kubernetes.core_namespace
                                }
                            },
                            "podSelector": {
                                "matchLabels": {
                                    "app.kubernetes.io/name": "agentrt-core",
                                    PREFIX + "scope": config.deployment_scope,
                                }
                            },
                        }
                    ],
                    "ports": [{"protocol": "TCP", "port": config.kubernetes.service_port}],
                }
            ],
        },
    }


def pod_spec(config: SandboxConfig, sandbox_id: str, name: str) -> dict[str, Any]:
    k = config.kubernetes
    context = {
        "runAsNonRoot": True,
        "allowPrivilegeEscalation": False,
        "privileged": False,
        "readOnlyRootFilesystem": True,
        "capabilities": {"drop": ["ALL"]},
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    env = [
        {"name": "SANDBOX_ID", "value": sandbox_id},
        {"name": "POD_UID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}}},
        {"name": "SERVICE_PORT", "value": str(k.service_port)},
        {"name": "COMMAND_TIMEOUT_S", "value": str(min(config.command_timeout_s, 120))},
        {"name": "CLEANUP_MARGIN_S", "value": str(config.cleanup_margin_s)},
        {"name": "JOURNAL_MAX_ENTRIES", "value": str(k.journal_max_entries)},
        {"name": "JOURNAL_MAX_BYTES", "value": str(k.journal_max_bytes)},
        {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
    ]
    # These configured limits apply to the execution container. The trusted
    # broker gets separate, fixed limits, included in Namespace quota accounting.
    executor_resources = {
        "requests": {
            "cpu": k.cpu_request,
            "memory": k.memory_request,
            "ephemeral-storage": k.ephemeral_storage_request,
        },
        "limits": {
            "cpu": k.cpu_limit,
            "memory": k.memory_limit,
            "ephemeral-storage": k.ephemeral_storage_limit,
        },
    }
    broker_resources = {
        "requests": {"cpu": "50m", "memory": "64Mi", "ephemeral-storage": "16Mi"},
        "limits": {"cpu": "250m", "memory": "256Mi", "ephemeral-storage": "64Mi"},
    }
    containers = []
    for role, uid, resources, mounts in (
        (
            "broker",
            10000,
            broker_resources,
            [
                {"name": "credential", "mountPath": "/run/credential", "readOnly": True},
                {"name": "control", "mountPath": "/tmp/control", "readOnly": True},
            ],
        ),
        (
            "executor",
            10001,
            executor_resources,
            [
                {"name": "workspace", "mountPath": "/workspace"},
                {"name": "tmp", "mountPath": "/tmp"},
                {"name": "control", "mountPath": "/tmp/control"},
            ],
        ),
    ):
        container: dict[str, Any] = {
            "name": role,
            "image": k.image,
            "imagePullPolicy": "IfNotPresent",
            "command": ["python", "-m", "agent_runtime.sandbox_server", role],
            "env": env,
            "securityContext": {**context, "runAsUser": uid, "runAsGroup": uid},
            "resources": resources,
            "volumeMounts": mounts,
        }
        if role == "broker":
            container["ports"] = [{"containerPort": k.service_port, "name": "http"}]
            container["readinessProbe"] = {
                "tcpSocket": {"port": "http"},
                "periodSeconds": 1,
                "timeoutSeconds": 1,
                "failureThreshold": 3,
            }
        containers.append(container)
    spec: dict[str, Any] = {
        "restartPolicy": "Never",
        "automountServiceAccountToken": False,
        "enableServiceLinks": False,
        "hostNetwork": False,
        "hostPID": False,
        "hostIPC": False,
        "shareProcessNamespace": False,
        "terminationGracePeriodSeconds": 1,
        "securityContext": {
            "runAsNonRoot": True,
            "fsGroup": 10000,
            "seccompProfile": {"type": "RuntimeDefault"},
        },
        "containers": containers,
        "volumes": [
            {"name": "workspace", "emptyDir": {"sizeLimit": k.workspace_size_limit}},
            {"name": "tmp", "emptyDir": {"sizeLimit": k.tmp_size_limit}},
            {"name": "control", "emptyDir": {"sizeLimit": "1Mi"}},
            {
                "name": "credential",
                "secret": {
                    "secretName": name,
                    "defaultMode": 0o440,
                    "items": [{"key": "token", "path": "token"}],
                },
            },
        ],
    }
    if k.runtime_class_name:
        spec["runtimeClassName"] = k.runtime_class_name
    return spec


def service_spec(config: SandboxConfig, sandbox_id: str) -> dict[str, Any]:
    return {
        "type": "ClusterIP",
        "selector": {
            PREFIX + "id": sandbox_id,
            PREFIX + "owner": OWNER,
            PREFIX + "scope": config.deployment_scope,
        },
        "ports": [
            {
                "name": "http",
                "port": config.kubernetes.service_port,
                "targetPort": config.kubernetes.service_port,
                "protocol": "TCP",
            }
        ],
    }
