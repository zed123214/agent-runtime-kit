"""M1 cold-start backend. One in-cluster Core owns immutable Pod generations."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import secrets
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from agent_runtime.core.sandbox.config import SandboxConfig
from agent_runtime.core.sandbox.kube_api import ApiError, KubeApi, KubernetesApi
from agent_runtime.core.sandbox.models import (
    ReconcileReport,
    SandboxConflictError,
    SandboxHandle,
    SandboxKey,
    SandboxProvisionError,
    SandboxSpec,
    SandboxStatus,
    WorkspaceLostError,
)
from agent_runtime.core.sandbox.pod_spec import (
    OWNER,
    POLICY_VERSION,
    PREFIX,
    comparable_pod_spec,
    network_policies,
    pod_spec,
    service_spec,
)


def canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


def sandbox_id_for(scope: str, key: SandboxKey) -> str:
    return hashlib.sha256(canonical([scope, key.kind, key.id])).hexdigest()[:32]


def _contains(actual: Any, expected: Any) -> bool:
    """Allow Kubernetes defaulted object keys, never extra list members."""
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(
            key in actual and _contains(actual[key], value) for key, value in expected.items()
        )
    if isinstance(expected, list):
        return (
            isinstance(actual, list)
            and len(actual) == len(expected)
            and all(_contains(a, b) for a, b in zip(actual, expected, strict=True))
        )
    return bool(actual == expected)


@dataclass
class _Plan:
    key: SandboxKey
    sandbox_id: str
    name: str
    created: str
    nonce: str
    token: str = field(repr=False)
    uids: dict[str, str] = field(default_factory=dict)
    handle: SandboxHandle | None = None
    failed: bool = False


class KubernetesBackend:
    def identity_for(self, key: SandboxKey) -> str:
        return sandbox_id_for(self.config.deployment_scope, key)

    def __init__(
        self,
        config: SandboxConfig,
        *,
        api: KubeApi | None = None,
        http: httpx.AsyncClient | None = None,
        ownership_key: bytes | None = None,
    ) -> None:
        self.config = config
        self.api = api or KubernetesApi(config.kubernetes.namespace)
        self.http = http or httpx.AsyncClient(
            trust_env=False, follow_redirects=False, limits=httpx.Limits(max_connections=64)
        )
        self._ownership_key = ownership_key
        self._owns_http = http is None
        self._plans: dict[SandboxKey, _Plan] = {}
        self._lost: set[SandboxKey] = set()
        self._locks: dict[SandboxKey, asyncio.Lock] = {}
        self._start_lock = asyncio.Lock()
        self._started = False
        self._garbage: dict[str, _Plan] = {}
        self._record_sink: Callable[[dict[str, object]], None] | None = None

    def set_record_sink(self, sink: Callable[[dict[str, object]], None]) -> None:
        self._record_sink = sink

    async def start(self) -> None:
        async with self._start_lock:
            if self._started:
                return
            if self._owns_http and self.http.is_closed:
                self.http = httpx.AsyncClient(
                    trust_env=False, follow_redirects=False, limits=httpx.Limits(max_connections=64)
                )
            if self._ownership_key is None:
                path = Path(self.config.kubernetes.ownership_key_file)
                try:
                    info = path.stat()
                    loaded_key = path.read_bytes()
                except OSError:
                    raise SandboxProvisionError(
                        "Core ownership key is missing or unreadable"
                    ) from None
                if info.st_mode & 0o007:
                    raise SandboxProvisionError("ownership key must not be world-readable")
                self._ownership_key = loaded_key
            if len(self._ownership_key) < 32:
                raise SandboxProvisionError("ownership key requires at least 32 bytes")
            await self._check_network_policy()
            self._started = True

    async def _check_network_policy(self) -> None:
        if not self.config.kubernetes.network_policy_verified:
            raise SandboxProvisionError("CNI enforcement has not been attested")
        response = await self.api.list("network_policy")
        actual = {
            item["metadata"]["name"]: item.get("spec", {}) for item in response.get("items", [])
        }
        expected = network_policies(self.config)
        # Policies are additive. An unexpected policy could reopen egress or
        # ingress, so validating only the named default-deny is insufficient.
        if set(actual) != set(expected):
            raise SandboxProvisionError(
                "Sandbox namespace must contain exactly the two managed policies"
            )
        for name, spec in expected.items():
            normalized = dict(actual[name])
            for direction in ("ingress", "egress"):
                if direction in spec and direction not in normalized:
                    normalized[direction] = []
            if normalized != spec:
                raise SandboxProvisionError(f"NetworkPolicy differs from required baseline: {name}")

    def _new_plan(self, key: SandboxKey) -> _Plan:
        sid = sandbox_id_for(self.config.deployment_scope, key)
        return _Plan(
            key,
            sid,
            "sb-" + sid,
            datetime.now(UTC).isoformat(),
            secrets.token_hex(16),
            secrets.token_urlsafe(32),
        )

    def _metadata(self, plan: _Plan, kind: str) -> dict[str, Any]:
        record = {
            "scope": self.config.deployment_scope,
            "key_kind": plan.key.kind,
            "key_id": plan.key.id,
            "sandbox_id": plan.sandbox_id,
            "kind": kind,
            "name": plan.name,
            "namespace": self.config.kubernetes.namespace,
            "created": plan.created,
            "nonce": plan.nonce,
            "image": self.config.kubernetes.image,
            "policy_version": POLICY_VERSION,
            "profile": hashlib.sha256(canonical(asdict(self.config))).hexdigest(),
        }
        assert self._ownership_key is not None
        serialized = canonical(record).decode()
        return {
            "name": plan.name,
            "namespace": self.config.kubernetes.namespace,
            "labels": {
                PREFIX + "owner": OWNER,
                PREFIX + "scope": self.config.deployment_scope,
                PREFIX + "id": plan.sandbox_id,
            },
            "annotations": {
                PREFIX + "record": serialized,
                PREFIX + "signature": hmac.new(
                    self._ownership_key, serialized.encode(), hashlib.sha256
                ).hexdigest(),
            },
        }

    def _trusted(self, obj: dict[str, Any], kind: str) -> dict[str, Any] | None:
        meta = obj.get("metadata", {})
        annotations = meta.get("annotations", {})
        raw = annotations.get(PREFIX + "record", "")
        signature = annotations.get(PREFIX + "signature", "")
        if not isinstance(raw, str) or not isinstance(signature, str):
            return None
        assert self._ownership_key is not None
        expected = hmac.new(self._ownership_key, raw.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(
            expected.encode("ascii"),
            signature.encode("utf-8", errors="surrogatepass"),
        ):
            return None
        try:
            record: dict[str, Any] = json.loads(raw)
            key = SandboxKey(record["key_kind"], record["key_id"])
            sid = sandbox_id_for(self.config.deployment_scope, key)
            if any(
                (
                    record["scope"] != self.config.deployment_scope,
                    record["kind"] != kind,
                    record["namespace"] != self.config.kubernetes.namespace,
                    record["sandbox_id"] != sid,
                    record["name"] != "sb-" + sid,
                    meta.get("name") != record["name"],
                    meta.get("namespace") != record["namespace"],
                )
            ):
                return None
            labels = meta.get("labels", {})
            if not _contains(
                labels,
                {
                    PREFIX + "owner": OWNER,
                    PREFIX + "scope": self.config.deployment_scope,
                    PREFIX + "id": sid,
                },
            ):
                return None
            if not meta.get("uid"):
                return None
            if kind == "pod" and any(
                c.get("image") != record["image"] for c in obj.get("spec", {}).get("containers", [])
            ):
                return None
            return record
        except (ValueError, KeyError, TypeError):
            return None

    def _validate(self, obj: dict[str, Any], plan: _Plan, kind: str) -> None:
        record = self._trusted(obj, kind)
        expected_meta = self._metadata(plan, kind)
        actual_record = obj.get("metadata", {}).get("annotations", {}).get(PREFIX + "record")
        if record is None or actual_record != expected_meta["annotations"][PREFIX + "record"]:
            raise SandboxConflictError(f"Refusing to adopt conflicting {kind} {plan.name}")
        uid = obj["metadata"]["uid"]
        if kind in plan.uids and plan.uids[kind] != uid:
            self._lost.add(plan.key)
            raise WorkspaceLostError(f"{kind} UID changed; create a new Session")
        if obj["metadata"].get("deletionTimestamp"):
            raise WorkspaceLostError(f"{kind} is terminating; create a new Session")
        if kind == "pod":
            spec = obj.get("spec", {})
            try:
                expected = comparable_pod_spec(pod_spec(self.config, plan.sandbox_id, plan.name))
                matches = _contains(comparable_pod_spec(spec), expected)
            except (ValueError, TypeError):
                matches = False
            if not matches or (spec.get("initContainers") or spec.get("ephemeralContainers")):
                raise SandboxConflictError(
                    "Pod security/image/resources differ from the owned template"
                )
        elif kind == "service":
            spec = obj.get("spec", {})
            owners = obj["metadata"].get("ownerReferences", [])
            expected_owner = [
                {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "name": plan.name,
                    "uid": plan.uids.get("pod"),
                    "controller": True,
                    "blockOwnerDeletion": False,
                }
            ]
            if not _contains(spec, service_spec(self.config, plan.sandbox_id)) or (
                spec.get("externalIPs")
                or spec.get("externalName")
                or spec.get("selector") != service_spec(self.config, plan.sandbox_id)["selector"]
                or not _contains(owners, expected_owner)
            ):
                raise SandboxConflictError("Service routing differs from the owned template")
        else:
            import base64

            expected_token = base64.b64encode(plan.token.encode()).decode()
            if (
                obj.get("immutable") is not True
                or obj.get("type") != "Opaque"
                or obj.get("data", {}).get("token") != expected_token
            ):
                raise SandboxConflictError("Sandbox credential differs from the owned generation")
        plan.uids[kind] = uid

    async def _get_or_create(self, plan: _Plan, kind: str) -> dict[str, Any]:
        import base64

        while True:  # Bounded by ensure's provisioning deadline.
            try:
                obj = await self.api.get(kind, plan.name)
                if obj is None:
                    if kind in plan.uids:
                        raise WorkspaceLostError(f"Owned {kind} disappeared")
                    meta = self._metadata(plan, kind)
                    if kind == "secret":
                        body = {
                            "apiVersion": "v1",
                            "kind": "Secret",
                            "metadata": meta,
                            "type": "Opaque",
                            "immutable": True,
                            "data": {"token": base64.b64encode(plan.token.encode()).decode()},
                        }
                    elif kind == "pod":
                        body = {
                            "apiVersion": "v1",
                            "kind": "Pod",
                            "metadata": meta,
                            "spec": pod_spec(self.config, plan.sandbox_id, plan.name),
                        }
                    else:
                        meta["ownerReferences"] = [
                            {
                                "apiVersion": "v1",
                                "kind": "Pod",
                                "name": plan.name,
                                "uid": plan.uids["pod"],
                                "controller": True,
                                "blockOwnerDeletion": False,
                            }
                        ]
                        body = {
                            "apiVersion": "v1",
                            "kind": "Service",
                            "metadata": meta,
                            "spec": service_spec(self.config, plan.sandbox_id),
                        }
                    obj = await self.api.create(kind, body)
                self._validate(obj, plan, kind)
                return obj
            except ApiError as exc:
                if exc.status not in (0, 409, 429, 500, 502, 503, 504):
                    raise SandboxProvisionError(str(exc)) from None
                await asyncio.sleep(0.25)

    def _pod_ready(self, pod: dict[str, Any], plan: _Plan) -> bool:
        self._validate(pod, plan, "pod")
        status = pod.get("status", {})
        terminated = [
            {
                "container": item.get("name"),
                **{
                    field: value
                    for field, value in item["state"]["terminated"].items()
                    if field in {"reason", "exitCode", "signal", "startedAt", "finishedAt"}
                },
            }
            for item in status.get("containerStatuses", [])
            if "terminated" in item.get("state", {})
        ]
        reason = status.get("reason") or next(
            (item["reason"] for item in terminated if item.get("reason") == "OOMKilled"),
            "pod_terminated",
        )
        if (terminated or status.get("phase") in ("Failed", "Succeeded")) and self._record_sink:
            self._record_sink(
                {
                    "type": "sandbox.pod_terminal",
                    "sandbox_id": plan.sandbox_id,
                    "pod_uid": pod["metadata"]["uid"],
                    "terminal_reason": reason,
                    "containers": terminated,
                    "ts": datetime.now(UTC).isoformat(),
                }
            )
        if status.get("phase") in ("Failed", "Succeeded"):
            raise WorkspaceLostError(f"workspace_lost: {reason}", terminal_reason=reason)
        for item in status.get("containerStatuses", []):
            state = item.get("state", {})
            if item.get("restartCount", 0) or "terminated" in state:
                reason = state.get("terminated", {}).get("reason", "generation_changed")
                raise WorkspaceLostError(f"workspace_lost: {reason}", terminal_reason=reason)
        return any(
            c.get("type") == "Ready" and c.get("status") == "True"
            for c in status.get("conditions", [])
        )

    async def _wait_ready(self, plan: _Plan, deadline: float) -> None:
        # A fresh list supplies both current state and a resourceVersion. Re-list
        # after disconnection/410 to avoid missing a transition while reconnecting.
        while time.monotonic() < deadline:
            try:
                listed = await self.api.list("pod", name=plan.name)
                pods = listed.get("items", [])
                if not pods:
                    raise WorkspaceLostError("Owned Pod disappeared while provisioning")
                if await self._checked_pod_ready(pods[0], plan):
                    return
                version = listed.get("metadata", {}).get("resourceVersion", "")
                seconds = max(1, min(10, int(deadline - time.monotonic())))
                async for event in self.api.watch_pods(plan.name, version, seconds):
                    if event["type"] == "ERROR":
                        raise ApiError(int(event["object"].get("code", 0)))
                    if event["type"] == "DELETED":
                        raise WorkspaceLostError("Owned Pod was deleted")
                    if event["type"] in ("ADDED", "MODIFIED") and await self._checked_pod_ready(
                        event["object"], plan
                    ):
                        return
            except ApiError as exc:
                if exc.status not in (0, 410, 429, 500, 502, 503, 504):
                    raise SandboxProvisionError(str(exc)) from None
            await asyncio.sleep(0.1)
        raise SandboxProvisionError("Pod Ready deadline expired")

    async def _checked_pod_ready(self, pod: dict[str, Any], plan: _Plan) -> bool:
        try:
            return self._pod_ready(pod, plan)
        except WorkspaceLostError as exc:
            if exc.terminal_reason not in {"pod_terminated", "Completed"}:
                raise
            # A gracefully stopped Worker can end with phase=Succeeded after
            # kubelet eviction. Scope event evidence to the immutable Pod UID.
            try:
                events = await self.api.events_for_pod(pod["metadata"]["uid"])
            except ApiError:
                raise exc from None
            for event in events.get("items", []):
                involved = event.get("involvedObject", {})
                component = event.get("reportingComponent") or event.get("source", {}).get(
                    "component"
                )
                if (
                    event.get("reason") == "Evicted"
                    and component == "kubelet"
                    and involved.get("uid") == pod["metadata"]["uid"]
                    and involved.get("kind") == "Pod"
                ):
                    if self._record_sink:
                        self._record_sink(
                            {
                                "type": "sandbox.pod_terminal",
                                "sandbox_id": plan.sandbox_id,
                                "pod_uid": pod["metadata"]["uid"],
                                "terminal_reason": "Evicted",
                                "reason_source": "kubelet_event",
                                "pod_phase": pod.get("status", {}).get("phase"),
                                "event_uid": event.get("metadata", {}).get("uid"),
                                "event_message": event.get("message", ""),
                                "ts": datetime.now(UTC).isoformat(),
                            }
                        )
                    raise WorkspaceLostError(
                        "workspace_lost: Evicted", terminal_reason="Evicted"
                    ) from None
            raise

    async def health(self, handle: SandboxHandle) -> dict[str, Any]:
        plan = self._plans[handle.key]
        assert handle.endpoint is not None
        async with self.http.stream(
            "GET",
            handle.endpoint + "/healthz",
            headers={
                "Authorization": "Bearer " + plan.token,
            },
            timeout=2,
        ) as response:
            if response.status_code in (409, 410):
                raise WorkspaceLostError("Worker generation is no longer available")
            if response.status_code != 200:
                raise SandboxProvisionError("Worker API is not ready")
            raw = bytearray()
            async for chunk in response.aiter_bytes(chunk_size=1024):
                raw.extend(chunk)
                if len(raw) > 4096:
                    raise SandboxConflictError("Unexpected Worker health response")
        try:
            data: dict[str, Any] = json.loads(raw)
            if (
                data["protocol"] != "1"
                or data["sandbox_id"] != handle.sandbox_id
                or data["pod_uid"] != handle.pod_uid
                or not isinstance(data["generation"], str)
                or not 1 <= len(data["generation"]) <= 128
            ):
                raise ValueError
            if handle.generation is not None and data["generation"] != handle.generation:
                raise WorkspaceLostError("Worker generation changed; create a new Session")
            return data
        except (ValueError, KeyError, TypeError):
            raise SandboxConflictError("Worker protocol/identity mismatch") from None

    async def ensure(self, key: SandboxKey, spec: SandboxSpec) -> SandboxHandle:
        async with self._locks.setdefault(key, asyncio.Lock()):
            if key in self._lost:
                raise WorkspaceLostError("workspace_lost; create a new Session")
            existing = self._plans.get(key)
            if existing is not None and existing.handle is not None and not existing.failed:
                await self.validate_handle(existing.handle)
                return existing.handle
            plan = existing or self._new_plan(key)
            self._plans[key] = plan
            if plan.failed:
                raise WorkspaceLostError("Provisioning ownership is closed; create a new Session")
            deadline = time.monotonic() + self.config.kubernetes.ready_timeout_s
            try:
                async with asyncio.timeout_at(deadline):
                    await self.start()
                    await self._check_network_policy()
                    await self._get_or_create(plan, "secret")
                    await self._get_or_create(plan, "pod")
                    service = await self._get_or_create(plan, "service")
                    await self._wait_ready(plan, deadline)
                    ip = service.get("spec", {}).get("clusterIP")
                    import ipaddress

                    address = ipaddress.ip_address(ip)
                    host = f"[{address}]" if address.version == 6 else str(address)
                    handle = SandboxHandle(
                        key,
                        plan.sandbox_id,
                        backend="kubernetes",
                        namespace=self.config.kubernetes.namespace,
                        pod_uid=plan.uids["pod"],
                        endpoint=f"http://{host}:{self.config.kubernetes.service_port}",
                        pod_name=plan.name,
                        image_digest=self.config.kubernetes.image,
                        policy_version=POLICY_VERSION,
                        resource_profile=hashlib.sha256(
                            canonical(asdict(self.config.kubernetes))
                        ).hexdigest()[:16],
                    )
                    plan.handle = handle
                    while True:
                        try:
                            health = await self.health(handle)
                            handle.generation = health["generation"]
                            return handle
                        except (httpx.HTTPError, SandboxProvisionError):
                            await asyncio.sleep(0.2)
            except BaseException as exc:
                plan.failed = True
                self._lost.add(key)
                self._garbage[plan.name] = plan
                cleanup = asyncio.create_task(self._destroy_plan(plan))
                try:
                    await asyncio.shield(cleanup)
                except BaseException:
                    # Retain signed partial ownership for subsequent reconcile.
                    cleanup.add_done_callback(
                        lambda task: None if task.cancelled() else task.exception()
                    )
                if isinstance(exc, TimeoutError):
                    raise SandboxProvisionError("Sandbox provisioning deadline expired") from None
                raise

    async def validate_handle(self, handle: SandboxHandle) -> None:
        if handle.key in self._lost:
            raise WorkspaceLostError("workspace_lost; create a new Session")
        plan = self._plans[handle.key]
        try:
            async with asyncio.timeout(self.config.kubernetes.ready_timeout_s):
                await self._check_network_policy()
                for kind in ("pod", "service", "secret"):
                    obj = await self.api.get(kind, plan.name)
                    if obj is None:
                        raise WorkspaceLostError(f"{kind} disappeared; create a new Session")
                    self._validate(obj, plan, kind)
                    if kind == "pod" and not await self._checked_pod_ready(obj, plan):
                        raise WorkspaceLostError("Pod is no longer Ready")
                await self.health(handle)
        except (WorkspaceLostError, SandboxConflictError):
            self._lost.add(handle.key)
            raise
        except (ApiError, httpx.HTTPError, TimeoutError) as exc:
            raise SandboxProvisionError("Pre-dispatch validation is unavailable") from exc

    async def terminal_failure(self, handle: SandboxHandle) -> WorkspaceLostError | None:
        """Observe a kernel/kubelet terminal cause before unknown-result cleanup.

        This does not recover the command's result or replay its side effects.
        A short bounded wait allows kubelet to publish a container OOM status.
        """
        try:
            async with asyncio.timeout(min(2.0, self.config.cleanup_margin_s)):
                while True:
                    pod = await self.api.get("pod", str(handle.pod_name))
                    if pod is None:
                        return None
                    try:
                        await self._checked_pod_ready(pod, self._plans[handle.key])
                    except WorkspaceLostError as exc:
                        if exc.terminal_reason in {"OOMKilled", "Evicted"}:
                            return exc
                        return None
                    await asyncio.sleep(0.1)
        except (ApiError, TimeoutError, SandboxConflictError):
            return None

    async def status(self, handle: SandboxHandle) -> SandboxStatus:
        if handle.status == SandboxStatus.TERMINATED:
            return SandboxStatus.TERMINATED
        try:
            await self.validate_handle(handle)
        except WorkspaceLostError:
            return SandboxStatus.FAILED
        return SandboxStatus.READY

    def credential(self, handle: SandboxHandle) -> str:
        return self._plans[handle.key].token

    def runtime_for(self, handle: SandboxHandle) -> Any:
        from agent_runtime.core.sandbox.remote import KubernetesRuntime

        return KubernetesRuntime(self, handle)

    async def _destroy_plan(self, plan: _Plan) -> None:
        errors: list[Exception] = []
        async with asyncio.timeout(self.config.cleanup_margin_s):
            for kind in ("pod", "service", "secret"):
                try:
                    obj = await self.api.get(kind, plan.name)
                    if obj is None:
                        continue
                    # A terminating object is eligible for deletion; creation's
                    # Ready validation intentionally rejects it instead.
                    record = self._trusted(obj, kind)
                    if record is None or record.get("nonce") != plan.nonce:
                        raise SandboxConflictError(f"Refusing to delete conflicting {kind}")
                    uid = obj["metadata"]["uid"]
                    if kind in plan.uids and uid != plan.uids[kind]:
                        raise SandboxConflictError(f"Refusing to delete replaced {kind} UID")
                    await self.api.delete(kind, plan.name, uid)
                except Exception as exc:
                    errors.append(exc)
            if not errors:
                # DELETE acceptance is not yet proof that the Pod/process cgroup
                # is gone. A bounded disappearance wait makes successful close
                # meaningful; a timeout remains retryable reconciliation debt.
                remaining = {"pod", "service", "secret"}
                while remaining:
                    for kind in tuple(remaining):
                        obj = await self.api.get(kind, plan.name)
                        if obj is None:
                            remaining.remove(kind)
                        elif kind in plan.uids and obj["metadata"]["uid"] != plan.uids[kind]:
                            raise SandboxConflictError("Resource replaced during deletion")
                    if remaining:
                        await asyncio.sleep(0.1)
        if errors:
            raise ExceptionGroup("Sandbox cleanup needs reconciliation", errors)

    async def destroy(self, handle: SandboxHandle, reason: str) -> None:
        plan = self._plans.get(handle.key)
        self._lost.add(handle.key)
        if plan is None:
            return
        plan.failed = True
        self._garbage[plan.name] = plan
        await self._destroy_plan(plan)
        handle.status = SandboxStatus.TERMINATED

    async def cleanup_key(self, key: SandboxKey) -> None:
        """Join/retry partial creation cleanup even when ensure returned no handle."""
        plan = self._plans.get(key)
        if plan is not None:
            plan.failed = True
            self._lost.add(key)
            self._garbage[plan.name] = plan
            await self._destroy_plan(plan)

    async def reconcile(self) -> ReconcileReport:
        await self.start()
        examined = destroyed = 0
        errors: list[str] = []
        active = {plan.name for plan in self._plans.values() if not plan.failed}
        selector = f"{PREFIX}owner={OWNER},{PREFIX}scope={self.config.deployment_scope}"
        now = datetime.now(UTC)
        for kind in ("pod", "service", "secret"):
            try:
                listed = await self.api.list(kind, selector=selector)
            except Exception:
                errors.append(f"list {kind} failed")
                continue
            for obj in listed.get("items", []):
                examined += 1
                meta = obj.get("metadata", {})
                name = meta.get("name", "")
                if name in active:
                    continue
                record = self._trusted(obj, kind)
                if record is None:
                    errors.append(f"untrusted {kind} {name}: left untouched")
                    continue
                try:
                    # Both signed intent time and API-assigned creation time
                    # must be outside grace; neither is sufficient alone.
                    timestamps = [
                        datetime.fromisoformat(record["created"]),
                        datetime.fromisoformat(meta["creationTimestamp"].replace("Z", "+00:00")),
                    ]
                    if (
                        any(
                            ts.tzinfo is None
                            or (now - ts).total_seconds() < self.config.reconcile_grace_s
                            for ts in timestamps
                        )
                        and name not in self._garbage
                    ):
                        continue
                    pending = self._garbage.get(name)
                    if pending is not None and (
                        record["nonce"] != pending.nonce
                        or (kind in pending.uids and meta["uid"] != pending.uids[kind])
                    ):
                        raise SandboxConflictError("replaced ownership")
                    await self.api.delete(kind, name, meta["uid"])
                    destroyed += 1
                    if self._record_sink is not None:
                        self._record_sink(
                            {
                                "type": "sandbox.reconcile_deleted",
                                "run_id": None,
                                "ts": datetime.now(UTC).isoformat(),
                                "sandbox_id": record["sandbox_id"],
                                "resource_kind": kind,
                                "resource_uid": meta["uid"],
                                "resource_name": name,
                                "deployment_scope": self.config.deployment_scope,
                                "terminal_reason": "orphan_or_pending_cleanup",
                            }
                        )
                except Exception:
                    errors.append(f"delete {kind} {name} failed; retained for reconciliation")
        return ReconcileReport(examined, destroyed, tuple(errors))

    async def aclose(self) -> None:
        await self.http.aclose()
        await self.api.close()
        self._started = False
