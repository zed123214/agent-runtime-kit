"""Application assembly; Kubernetes remains an optional, lazy import."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from agent_runtime.core.sandbox.config import SandboxConfig
from agent_runtime.core.sandbox.manager import SandboxManager
from agent_runtime.core.sandbox.models import SandboxSpec


def create_sandbox_manager(
    config: SandboxConfig,
    data_root: Path,
    *,
    trace_sink: Callable[[dict[str, object]], None] | None = None,
) -> SandboxManager:
    if config.backend == "local":
        return SandboxManager()
    from agent_runtime.core.sandbox.kubernetes import KubernetesBackend
    from agent_runtime.core.sandbox.pod_spec import POLICY_VERSION

    return SandboxManager(
        KubernetesBackend(config),
        spec=SandboxSpec(
            backend="kubernetes",
            workspace_policy="emptydir-workspace",
            policy_version=POLICY_VERSION,
        ),
        queue_timeout_s=config.queue_timeout_s,
        idle_timeout_s=config.idle_timeout_s,
        provision_timeout_s=config.kubernetes.ready_timeout_s,
        lifecycle_path=data_root / "sandboxes" / "lifecycle.jsonl",
        trace_sink=trace_sink,
    )
