"""API/HTTP fake contracts only; these are not cluster isolation evidence."""

from __future__ import annotations

import asyncio
import copy
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from agent_runtime.core.sandbox.config import SandboxConfig
from agent_runtime.core.sandbox.kube_api import ApiError
from agent_runtime.core.sandbox.kubernetes import KubernetesBackend, sandbox_id_for
from agent_runtime.core.sandbox.models import (
    SandboxConflictError,
    SandboxKey,
    SandboxProvisionError,
    SandboxSpec,
    WorkspaceLostError,
)
from agent_runtime.core.sandbox.pod_spec import PREFIX, network_policies


def config() -> SandboxConfig:
    result = SandboxConfig(backend="kubernetes", deployment_scope="unit")
    result.kubernetes.image = "example/worker@sha256:" + "a" * 64
    result.kubernetes.network_policy_verified = True
    return result


class FakeApi:
    def __init__(self, settings: SandboxConfig) -> None:
        self.objects: dict[tuple[str, str], dict[str, Any]] = {}
        self.created: list[str] = []
        self.deleted: list[tuple[str, str]] = []
        self.settings = settings
        self.fail_create = ""
        self.lose_response = ""
        self.pending = False
        self.watch_disconnect = False
        self.watches = 0
        self.lists = 0
        self.fail_delete = False
        self.events: list[dict[str, Any]] = []

    async def events_for_pod(self, uid: str) -> dict[str, Any]:
        return {"items": self.events}

    async def get(self, kind: str, name: str) -> dict[str, Any] | None:
        return copy.deepcopy(self.objects.get((kind, name)))

    async def create(self, kind: str, body: dict[str, Any]) -> dict[str, Any]:
        if kind == self.fail_create:
            raise ApiError(403)
        self.created.append(kind)
        obj = copy.deepcopy(body)
        name = obj["metadata"]["name"]
        obj["metadata"].update(
            uid=f"uid-{kind}-{len(self.created)}",
            resourceVersion="1",
            creationTimestamp=datetime.now(UTC).isoformat(),
        )
        if kind == "pod":
            obj["status"] = {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "False" if self.pending else "True"}],
                "containerStatuses": [
                    {"name": "broker", "restartCount": 0, "state": {"running": {}}},
                    {"name": "executor", "restartCount": 0, "state": {"running": {}}},
                ],
            }
        if kind == "service":
            obj["spec"]["clusterIP"] = "10.0.0.8"
        self.objects[kind, name] = obj
        if kind == self.lose_response:
            self.lose_response = ""
            raise ApiError(503)
        return copy.deepcopy(obj)

    async def delete(self, kind: str, name: str, uid: str) -> None:
        if self.fail_delete:
            self.fail_delete = False
            raise ApiError(503)
        obj = self.objects.get((kind, name))
        if obj is None:
            return
        if obj["metadata"]["uid"] != uid:
            raise ApiError(409)
        self.deleted.append((kind, name))
        del self.objects[kind, name]

    async def list(self, kind: str, selector: str = "", name: str = "") -> dict[str, Any]:
        self.lists += 1
        if kind == "network_policy":
            items = [
                {"metadata": {"name": key}, "spec": spec}
                for key, spec in network_policies(self.settings).items()
            ]
        else:
            items = [
                copy.deepcopy(obj)
                for (k, n), obj in self.objects.items()
                if k == kind and (not name or n == name)
            ]
            if selector:
                labels = dict(part.split("=", 1) for part in selector.split(","))
                items = [
                    obj
                    for obj in items
                    if all(obj["metadata"].get("labels", {}).get(k) == v for k, v in labels.items())
                ]
        return {"items": items, "metadata": {"resourceVersion": str(self.lists)}}

    async def watch_pods(self, name: str, version: str, seconds: int) -> Any:
        self.watches += 1
        if self.watch_disconnect:
            self.watch_disconnect = False
            raise ApiError(410)
        obj = self.objects["pod", name]
        obj["status"]["conditions"][0]["status"] = "True"
        yield {"type": "MODIFIED", "object": copy.deepcopy(obj)}

    async def close(self) -> None:
        pass


def make_backend(settings: SandboxConfig | None = None) -> tuple[KubernetesBackend, FakeApi]:
    settings = settings or config()
    api = FakeApi(settings)
    backend: KubernetesBackend

    async def health(request: httpx.Request) -> httpx.Response:
        plan = next(plan for plan in backend._plans.values() if plan.handle is not None)
        assert request.headers["authorization"] == "Bearer " + plan.token
        return httpx.Response(
            200,
            json={
                "protocol": "1",
                "sandbox_id": plan.sandbox_id,
                "pod_uid": plan.uids["pod"],
                "generation": "generation-1",
            },
        )

    backend = KubernetesBackend(
        settings,
        api=api,
        ownership_key=b"x" * 32,
        http=httpx.AsyncClient(transport=httpx.MockTransport(health)),
    )
    return backend, api


@pytest.mark.parametrize("phase", ["Running", "Failed"])
async def test_oom_cause_is_recorded_before_unknown_result_cleanup(phase: str) -> None:
    backend, api = make_backend()
    rows: list[dict[str, object]] = []
    backend.set_record_sink(rows.append)
    try:
        handle = await backend.ensure(SandboxKey("session", "oom-observation"), SandboxSpec())
        pod = api.objects["pod", str(handle.pod_name)]
        pod["status"]["phase"] = phase
        pod["status"]["containerStatuses"][1]["state"] = {
            "terminated": {"reason": "OOMKilled", "exitCode": 137, "signal": 9}
        }
        error = await backend.terminal_failure(handle)
        assert error is not None and error.terminal_reason == "OOMKilled"
        assert error.code == "workspace_lost"
        assert rows[-1]["terminal_reason"] == "OOMKilled"
        assert rows[-1]["pod_uid"] == handle.pod_uid
        assert api.deleted == []
    finally:
        await backend.http.aclose()


@pytest.mark.parametrize("same_uid", [False, True])
async def test_completed_worker_uses_only_its_own_kubelet_eviction_event(same_uid: bool) -> None:
    backend, api = make_backend()
    rows: list[dict[str, object]] = []
    backend.set_record_sink(rows.append)
    try:
        handle = await backend.ensure(SandboxKey("session", "storage-replay"), SandboxSpec())
        pod = api.objects["pod", str(handle.pod_name)]
        # Replays the real kind observation: graceful Worker exits overwrite the
        # Pod phase to Succeeded, while the UID-scoped kubelet Event says Evicted.
        pod["status"]["phase"] = "Succeeded"
        for container in pod["status"]["containerStatuses"]:
            container["state"] = {"terminated": {"reason": "Completed", "exitCode": 0}}
        api.events = [
            {
                "metadata": {"uid": "eviction-event"},
                "reason": "Evicted",
                "message": 'Usage of EmptyDir volume "workspace" exceeds the limit "512Mi".',
                "source": {"component": "kubelet"},
                "involvedObject": {
                    "kind": "Pod",
                    "uid": handle.pod_uid if same_uid else "older-pod",
                },
            }
        ]
        with pytest.raises(WorkspaceLostError) as error:
            await backend.validate_handle(handle)
        assert error.value.terminal_reason == ("Evicted" if same_uid else "pod_terminated")
        assert any(r.get("reason_source") == "kubelet_event" for r in rows) == same_uid
    finally:
        await backend.http.aclose()


async def test_concurrent_ensure_and_lost_create_response_do_not_duplicate() -> None:
    backend, api = make_backend()
    api.lose_response = "pod"
    key = SandboxKey("session", "chat")
    try:
        first, second = await asyncio.gather(
            backend.ensure(key, SandboxSpec()), backend.ensure(key, SandboxSpec())
        )
        assert first is second
        assert api.created == ["secret", "pod", "service"]
        assert first.generation == "generation-1"
        await backend.destroy(first, "close")
        await backend.destroy(first, "close")
        assert api.objects == {}
    finally:
        await backend.aclose()


async def test_ready_watch_410_relists_and_checks_worker_api() -> None:
    backend, api = make_backend()
    api.pending = api.watch_disconnect = True
    try:
        handle = await backend.ensure(SandboxKey("session", "watch"), SandboxSpec())
        assert api.watches == 2
        assert handle.generation == "generation-1"
        await backend.destroy(handle, "done")
    finally:
        await backend.aclose()


async def test_foreign_pod_conflict_is_never_adopted_or_deleted() -> None:
    backend, api = make_backend()
    key = SandboxKey("session", "conflict")
    name = "sb-" + sandbox_id_for("unit", key)
    foreign = {"metadata": {"name": name, "uid": "foreign"}}
    api.objects["pod", name] = foreign
    try:
        with pytest.raises(SandboxConflictError):
            await backend.ensure(key, SandboxSpec())
        assert api.objects["pod", name] == foreign
        assert not any(kind == "pod" for kind, _ in api.deleted)
        assert ("secret", name) not in api.objects
    finally:
        await backend.aclose()


@pytest.mark.parametrize("failure", ["lost", "uid", "restart", "oom", "image"])
async def test_invalid_generation_never_recreates_empty_workspace(failure: str) -> None:
    backend, api = make_backend()
    key = SandboxKey("session", failure)
    try:
        handle = await backend.ensure(key, SandboxSpec())
        name = str(handle.pod_name)
        if failure == "lost":
            del api.objects["pod", name]
        else:
            pod = api.objects["pod", name]
            if failure == "uid":
                pod["metadata"]["uid"] = "replacement"
            elif failure == "restart":
                pod["status"]["containerStatuses"][0]["restartCount"] = 1
            elif failure == "oom":
                pod["status"]["containerStatuses"][1]["state"] = {
                    "terminated": {"reason": "OOMKilled"}
                }
            else:
                pod["spec"]["containers"][1]["image"] = "evil:latest"
        with pytest.raises((WorkspaceLostError, SandboxConflictError)):
            await backend.ensure(key, SandboxSpec())
        assert api.created.count("pod") == 1
    finally:
        await backend.aclose()


async def test_partial_creation_failure_cleans_owned_resources() -> None:
    backend, api = make_backend()
    api.fail_create = "service"
    try:
        with pytest.raises(SandboxProvisionError):
            await backend.ensure(SandboxKey("session", "partial"), SandboxSpec())
        assert api.objects == {}
    finally:
        await backend.aclose()


async def test_destroy_failure_is_reconciled_without_erasing_debt() -> None:
    backend, api = make_backend()
    try:
        handle = await backend.ensure(SandboxKey("session", "delete"), SandboxSpec())
        api.fail_delete = True
        with pytest.raises(ExceptionGroup):
            await backend.destroy(handle, "close")
        report = await backend.reconcile()
        assert report.destroyed == 1
        assert api.objects == {}
        await backend.destroy(handle, "retry")
    finally:
        await backend.aclose()


@pytest.mark.parametrize("signature", ["bad", "不可信签名"])
async def test_startup_scan_requires_signature_scope_and_grace(signature: str) -> None:
    backend, api = make_backend()
    try:
        plan = backend._new_plan(SandboxKey("session", "orphan"))
        plan.created = "2020-01-01T00:00:00+00:00"
        await backend.start()
        await backend._get_or_create(plan, "secret")
        obj = api.objects["secret", plan.name]
        obj["metadata"]["creationTimestamp"] = plan.created
        stranger = copy.deepcopy(obj)
        stranger["metadata"]["name"] = "foreign"
        stranger["metadata"]["annotations"][PREFIX + "signature"] = signature
        api.objects["secret", "foreign"] = stranger
        report = await backend.reconcile()
        assert report.destroyed == 1
        assert ("secret", "foreign") in api.objects
        assert report.errors
    finally:
        await backend.aclose()


async def test_pod_template_keeps_token_out_of_command_container() -> None:
    backend, api = make_backend()
    try:
        handle = await backend.ensure(SandboxKey("session", "security"), SandboxSpec())
        pod = api.objects["pod", str(handle.pod_name)]["spec"]
        broker, executor = pod["containers"]
        assert pod["automountServiceAccountToken"] is False
        assert pod["restartPolicy"] == "Never"
        assert pod["shareProcessNamespace"] is False
        assert broker["securityContext"]["runAsUser"] != executor["securityContext"]["runAsUser"]
        assert all(mount["name"] != "credential" for mount in executor["volumeMounts"])
        assert all(mount["name"] != "workspace" for mount in broker["volumeMounts"])
        assert all("hostPath" not in volume for volume in pod["volumes"])
        for container in pod["containers"]:
            security = container["securityContext"]
            assert security["capabilities"] == {"drop": ["ALL"]}
            assert security["readOnlyRootFilesystem"] and not security["allowPrivilegeEscalation"]
            assert security["seccompProfile"]["type"] == "RuntimeDefault"
            assert "ephemeral-storage" in container["resources"]["limits"]
        await backend.destroy(handle, "done")
    finally:
        await backend.aclose()


async def test_api_default_omission_and_quantity_canonicalization_are_compatible() -> None:
    settings = config()
    settings.kubernetes.cpu_limit = "1000m"
    backend, api = make_backend(settings)
    key = SandboxKey("session", "api-defaults")
    try:
        handle = await backend.ensure(key, SandboxSpec())
        spec = api.objects["pod", str(handle.pod_name)]["spec"]
        for field in ("hostNetwork", "hostPID", "hostIPC"):
            del spec[field]
        spec["containers"][1]["resources"]["limits"]["cpu"] = "1"
        await backend.validate_handle(handle)
        spec["hostPID"] = True
        with pytest.raises(SandboxConflictError):
            await backend.validate_handle(handle)
    finally:
        await backend.aclose()
