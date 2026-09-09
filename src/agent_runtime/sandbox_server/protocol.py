"""Versioned Worker DTOs; no Kubernetes or web-server imports."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

PROTOCOL_VERSION = "1"
MAX_REQUEST_BYTES = 6 * 1024 * 1024 + 16384
MAX_RESULT_BYTES = 4 * 1024 * 1024
FILE_TIMEOUT_S = 10.0


class Identity(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    sandbox_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    run_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
    tool_call_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
    attempt: int = Field(default=1, ge=1, le=10000)

    @property
    def key(self) -> tuple[str, str, str]:
        return self.sandbox_id, self.run_id, self.tool_call_id


class Operation(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    identity: Identity
    pod_uid: str = Field(min_length=1, max_length=128)
    generation: str = Field(min_length=1, max_length=128)
    operation: Literal["exec", "read_text", "write_text", "list_dir"]
    command: str = Field(default="", max_length=65536)
    timeout_s: float = Field(default=60.0, gt=0, le=120)
    path: str = Field(default=".", max_length=4096)
    content: str = Field(default="", max_length=1024 * 1024)
    max_depth: int = Field(default=2, ge=1, le=4)

    def response_timeout_s(self, cleanup_s: float) -> float:
        """Shared Core/broker budget; file I/O never inherits command policy."""
        execution_s = self.timeout_s if self.operation == "exec" else FILE_TIMEOUT_S
        return execution_s + cleanup_s + 2

    @model_validator(mode="after")
    def byte_limits(self) -> Operation:
        if len(self.path.encode("utf-8")) > 4096:
            raise ValueError("path exceeds 4096 UTF-8 bytes")
        if len(self.content.encode("utf-8")) > 1024 * 1024:
            raise ValueError("write exceeds 1 MiB")
        if len(self.command.encode("utf-8")) > 65536:
            raise ValueError("command exceeds 64 KiB")
        return self

    @property
    def input_hash(self) -> str:
        # attempt is a transport attempt, never a new side-effect identity.
        raw = self.model_dump(exclude={"identity", "pod_uid", "generation"})
        return hashlib.sha256(encode(raw)).hexdigest()


def encode(value: dict[str, Any]) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def error_result(code: str, message: str) -> dict[str, Any]:
    return {
        "content": message,
        "is_error": True,
        "error_type": code,
        "terminal_reason": code,
        "truncated": False,
    }
