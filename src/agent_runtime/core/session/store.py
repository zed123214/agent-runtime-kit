from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from agent_runtime.core.session.model import Session

logger = logging.getLogger(__name__)

MessageContent = str | list[dict[str, Any]]


class TranscriptStoreError(RuntimeError):
    code = "transcript_store_error"


class TranscriptCorruptionError(TranscriptStoreError):
    code = "transcript_corruption"


class TranscriptConflictError(TranscriptStoreError):
    code = "transcript_conflict"


@dataclass(frozen=True, slots=True)
class TranscriptCommit:
    transcript_commit_count: int
    transcript_commit_hash: str
    appended_count: int = 0


def _canonical_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    canonical: list[dict[str, Any]] = []
    for message in messages:
        if "role" not in message or "content" not in message:
            raise ValueError("transcript messages require role and content")
        role = str(message["role"])
        if role not in ("user", "assistant"):
            raise ValueError(f"unsupported transcript role: {role}")
        canonical.append({"role": role, "content": message["content"]})
    return canonical


def canonical_transcript_commit(messages: list[dict[str, Any]]) -> TranscriptCommit:
    canonical = _canonical_messages(messages)
    try:
        payload = json.dumps(
            canonical,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("transcript messages must be JSON serializable") from exc
    return TranscriptCommit(
        transcript_commit_count=len(canonical),
        transcript_commit_hash=hashlib.sha256(payload).hexdigest(),
    )


# 返回当前 UTC 时间的 ISO 8601 字符串
def _now() -> str:
    return datetime.now(UTC).isoformat()


def _read_committed_rows(path: Path) -> tuple[list[dict[str, Any]], bytes]:
    if not path.exists():
        return [], b""

    payload = path.read_bytes()
    if payload and not payload.endswith(b"\n"):
        payload = payload[: payload.rfind(b"\n") + 1]

    rows: list[dict[str, Any]] = []
    lines = payload.split(b"\n")
    for line_no, raw_line in enumerate(lines, start=1):
        if not raw_line and line_no == len(lines):
            continue
        if raw_line.endswith(b"\r"):
            raw_line = raw_line[:-1]
        if not raw_line:
            raise TranscriptCorruptionError(f"Empty transcript row at line {line_no}.")
        try:
            row = json.loads(raw_line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TranscriptCorruptionError(f"Invalid transcript row at line {line_no}.") from exc
        if not isinstance(row, dict):
            raise TranscriptCorruptionError(
                f"Transcript row at line {line_no} must be a JSON object."
            )
        rows.append(row)
    return rows, payload


def _serialize_rows(rows: list[dict[str, Any]]) -> bytes:
    try:
        return b"".join(
            (
                json.dumps(
                    row,
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode("utf-8")
            for row in rows
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("transcript rows must be JSON serializable") from exc


def _atomic_replace(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


class SessionStore:
    # 初始化 session 文件存储根目录
    def __init__(self, root: Path) -> None:
        self._root = root.expanduser()
        self._root.mkdir(parents=True, exist_ok=True)

    # 返回指定 session 的目录路径
    def session_dir(self, sid: str) -> Path:
        return self._root / sid

    # 返回指定 session 下的 runs 目录路径
    def runs_dir(self, sid: str) -> Path:
        return self.session_dir(sid) / "runs"

    # 将 session meta 写入 meta.json
    def write_meta(self, session: Session) -> None:
        path = self.session_dir(session.id)
        path.mkdir(parents=True, exist_ok=True)
        (path / "meta.json").write_text(
            json.dumps(session.to_dict(), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    # 从 meta.json 读取 session meta
    def read_meta(self, sid: str) -> Session:
        data = json.loads((self.session_dir(sid) / "meta.json").read_text(encoding="utf-8"))
        return Session.from_dict(data)

    # 追加一条 Anthropic API 消息到 thread.jsonl
    def append_message(
        self,
        sid: str,
        role: str,
        content: MessageContent,
        run_id: str | None = None,
    ) -> None:
        row: dict[str, Any] = {"ts": _now(), "role": role, "content": content}
        if run_id is not None:
            row["run_id"] = run_id
        path = self.session_dir(sid) / "thread.jsonl"
        _rows, committed = _read_committed_rows(path)
        _atomic_replace(path, committed + _serialize_rows([row]))

    # 批量追加一次 run 新产生的消息到 thread.jsonl
    def append_messages(
        self,
        sid: str,
        messages: list[dict[str, Any]],
        run_id: str,
    ) -> TranscriptCommit:
        expected = _canonical_messages(messages)
        canonical_commit = canonical_transcript_commit(expected)
        path = self.session_dir(sid) / "thread.jsonl"
        rows, committed = _read_committed_rows(path)
        existing = [
            {"role": str(row.get("role", "")), "content": row.get("content", "")}
            for row in rows
            if row.get("run_id") == run_id
        ]

        if existing == expected:
            return canonical_commit
        if len(existing) > len(expected) or existing != expected[: len(existing)]:
            raise TranscriptConflictError(
                f"Transcript rows for run {run_id!r} do not match the canonical increment."
            )

        if existing:
            target_indices = [
                index for index, row in enumerate(rows) if row.get("run_id") == run_id
            ]
            if target_indices[-1] != len(rows) - 1:
                raise TranscriptConflictError(
                    f"Transcript rows for run {run_id!r} are not a recoverable suffix."
                )

        missing = expected[len(existing) :]
        new_rows = [
            {
                "ts": _now(),
                "role": message["role"],
                "content": message["content"],
                "run_id": run_id,
            }
            for message in missing
        ]
        if new_rows:
            _atomic_replace(path, committed + _serialize_rows(new_rows))
        return TranscriptCommit(
            transcript_commit_count=canonical_commit.transcript_commit_count,
            transcript_commit_hash=canonical_commit.transcript_commit_hash,
            appended_count=len(new_rows),
        )

    def transcript_commit(self, sid: str, run_id: str) -> TranscriptCommit:
        rows, _committed = _read_committed_rows(self.session_dir(sid) / "thread.jsonl")
        messages = [
            {"role": str(row.get("role", "")), "content": row.get("content", "")}
            for row in rows
            if row.get("run_id") == run_id
        ]
        return canonical_transcript_commit(messages)

    def read_run_messages_strict(self, sid: str, run_id: str) -> list[dict[str, Any]]:
        """Read the committed increment for one durable run without hiding conflicts."""

        rows, _committed = _read_committed_rows(self.session_dir(sid) / "thread.jsonl")
        target_indices = [index for index, row in enumerate(rows) if row.get("run_id") == run_id]
        if target_indices and target_indices[-1] != len(rows) - 1:
            raise TranscriptConflictError(
                f"Transcript rows for run {run_id!r} are not a recoverable suffix."
            )
        messages = [
            {"role": row.get("role"), "content": row.get("content")}
            for row in rows
            if row.get("run_id") == run_id
        ]
        try:
            return _canonical_messages(messages)
        except ValueError as exc:
            raise TranscriptCorruptionError(
                f"Transcript rows for run {run_id!r} are invalid."
            ) from exc

    # 读取完整 thread 并返回可直接传给 Anthropic 的 messages
    def read_messages(self, sid: str) -> list[dict[str, Any]]:
        path = self.session_dir(sid) / "thread.jsonl"
        if not path.exists():
            return []

        messages: list[dict[str, Any]] = []
        for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("skip broken thread row sid=%s line=%s", sid, line_no)
                continue
            role = row.get("role")
            if role not in ("user", "assistant"):
                logger.warning(
                    "skip unknown thread role sid=%s line=%s role=%s",
                    sid,
                    line_no,
                    role,
                )
                continue
            messages.append({"role": role, "content": row.get("content", "")})

        messages = self._trim_orphan_tool_use(messages)
        from agent_runtime.core.compact.budget import truncate_tool_results

        return truncate_tool_results(messages)

    def read_messages_strict(self, sid: str) -> list[dict[str, Any]]:
        """Read a durable transcript without skipping corrupt committed rows."""

        rows, _committed = _read_committed_rows(self.session_dir(sid) / "thread.jsonl")
        messages: list[dict[str, Any]] = []
        for line_no, row in enumerate(rows, start=1):
            role = row.get("role")
            if role not in ("user", "assistant") or "content" not in row:
                raise TranscriptCorruptionError(
                    f"Invalid committed transcript message at line {line_no}."
                )
            messages.append({"role": role, "content": row["content"]})

        balanced_messages = self._trim_orphan_tool_use(messages)
        if len(balanced_messages) != len(messages):
            raise TranscriptConflictError("Durable transcript ends with an orphan tool_use.")
        from agent_runtime.core.compact.budget import truncate_tool_results

        return truncate_tool_results(balanced_messages)

    # 裁掉尾部未配对 tool_use 以及其后的消息，避免 Anthropic messages.invalid
    def _trim_orphan_tool_use(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        pending: set[str] = set()
        last_balanced = 0
        for idx, msg in enumerate(messages, start=1):
            content = msg.get("content")
            if isinstance(content, list):
                if msg.get("role") == "assistant":
                    for block in content:
                        if block.get("type") == "tool_use":
                            pending.add(str(block.get("id", "")))
                elif msg.get("role") == "user":
                    for block in content:
                        if block.get("type") == "tool_result":
                            pending.discard(str(block.get("tool_use_id", "")))
            if not pending:
                last_balanced = idx
        if pending:
            logger.warning("trim orphan tool_use blocks from thread")
            return messages[:last_balanced]
        return messages

    # 将压缩后的消息对覆盖写入 thread.jsonl，原文件备份为 thread_<ts>.jsonl.bak
    def write_compacted(self, sid: str, messages: list[dict[str, Any]]) -> None:
        path = self.session_dir(sid) / "thread.jsonl"
        ts_str = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        bak = self.session_dir(sid) / f"thread_{ts_str}.jsonl.bak"
        if path.exists():
            path.rename(bak)
        with path.open("w", encoding="utf-8") as f:
            for msg in messages:
                row: dict[str, Any] = {"ts": _now(), "role": msg["role"], "content": msg["content"]}
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    # 读取 notes.md 全文，文件不存在时返回空字符串
    def read_notes(self, sid: str) -> str:
        path = self.session_dir(sid) / "notes.md"
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8")

    # 将一条主动笔记追加到 notes.md
    def append_note(self, sid: str, content: str, run_id: str) -> None:
        path = self.session_dir(sid)
        path.mkdir(parents=True, exist_ok=True)
        with (path / "notes.md").open("a", encoding="utf-8") as f:
            f.write(f"## Note ({_now()}, {run_id})\n{content}\n\n")
