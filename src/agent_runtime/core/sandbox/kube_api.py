"""Small Kubernetes SDK boundary, suitable for deterministic fake API contracts."""

from __future__ import annotations

import importlib
from collections.abc import AsyncIterator
from typing import Any, Protocol


class ApiError(RuntimeError):
    def __init__(self, status: int) -> None:
        self.status = status
        # Do not propagate SDK exception bodies (Secret responses may be present).
        super().__init__(f"Kubernetes API status {status}")


class KubeApi(Protocol):
    async def get(self, kind: str, name: str) -> dict[str, Any] | None: ...
    async def create(self, kind: str, body: dict[str, Any]) -> dict[str, Any]: ...
    async def delete(self, kind: str, name: str, uid: str) -> None: ...
    async def list(self, kind: str, selector: str = "", name: str = "") -> dict[str, Any]: ...
    async def events_for_pod(self, uid: str) -> dict[str, Any]: ...
    def watch_pods(
        self, name: str, version: str, seconds: int
    ) -> AsyncIterator[dict[str, Any]]: ...
    async def close(self) -> None: ...


class KubernetesApi:
    def __init__(self, namespace: str) -> None:
        self.namespace = namespace
        self._client: Any = None
        self._core: Any = None
        self._network: Any = None
        self._watch: Any = None

    def _connect(self) -> None:
        if self._client is not None:
            return
        client = importlib.import_module("kubernetes_asyncio.client")
        config = importlib.import_module("kubernetes_asyncio.config")
        self._watch = importlib.import_module("kubernetes_asyncio.watch")
        # M1 Core is in-cluster. Never read a host kubeconfig or silently switch
        # context. Local development keeps the Local backend.
        config.load_incluster_config()
        self._client = client.ApiClient()
        self._core = client.CoreV1Api(self._client)
        self._network = client.NetworkingV1Api(self._client)

    async def _call(self, verb: str, kind: str, **kwargs: Any) -> Any:
        self._connect()
        api = self._network if kind == "network_policy" else self._core
        try:
            result = await getattr(api, f"{verb}_namespaced_{kind}")(
                namespace=self.namespace, _request_timeout=10, **kwargs
            )
            return self._client.sanitize_for_serialization(result)
        except Exception as exc:
            raise ApiError(int(getattr(exc, "status", 0) or 0)) from None

    async def get(self, kind: str, name: str) -> dict[str, Any] | None:
        try:
            result: dict[str, Any] = await self._call("read", kind, name=name)
            return result
        except ApiError as exc:
            if exc.status == 404:
                return None
            raise

    async def create(self, kind: str, body: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = await self._call("create", kind, body=body)
        return result

    async def delete(self, kind: str, name: str, uid: str) -> None:
        try:
            await self._call(
                "delete",
                kind,
                name=name,
                body={
                    # Keep the Pod's short graceful deletion path. Force deletion
                    # can erase its API identity before the kubelet stops processes.
                    "apiVersion": "v1",
                    "kind": "DeleteOptions",
                    "gracePeriodSeconds": 1 if kind == "pod" else 0,
                    "propagationPolicy": "Background",
                    "preconditions": {"uid": uid},
                },
            )
        except ApiError as exc:
            if exc.status != 404:
                raise

    async def list(self, kind: str, selector: str = "", name: str = "") -> dict[str, Any]:
        result: dict[str, Any] = await self._call(
            "list",
            kind,
            label_selector=selector,
            field_selector=f"metadata.name={name}" if name else "",
        )
        return result

    async def watch_pods(
        self, name: str, version: str, seconds: int
    ) -> AsyncIterator[dict[str, Any]]:
        self._connect()
        watch = self._watch.Watch()
        try:
            async for event in watch.stream(
                self._core.list_namespaced_pod,
                namespace=self.namespace,
                field_selector=f"metadata.name={name}",
                resource_version=version,
                timeout_seconds=seconds,
                _request_timeout=seconds + 2,
            ):
                yield {"type": event["type"], "object": event["raw_object"]}
        except Exception as exc:
            raise ApiError(int(getattr(exc, "status", 0) or 0)) from None
        finally:
            await watch.close()

    async def events_for_pod(self, uid: str) -> dict[str, Any]:
        result: dict[str, Any] = await self._call(
            "list",
            "event",
            field_selector=f"involvedObject.uid={uid},involvedObject.kind=Pod",
            limit=100,
        )
        return result

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None
