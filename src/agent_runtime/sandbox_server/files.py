"""Linux dirfd traversal. Every component, including the final open, is nofollow.

Reject all symlinks (even in-workspace ones) to keep a simple auditable boundary.
The root is a separate emptyDir mount, so a renamed directory cannot be moved to
another writable filesystem. Neither resolve()+open() nor string prefix checks
are used to authorize file access.
"""

from __future__ import annotations

import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager

from agent_runtime.core.sandbox.models import FileResult, ListResult

_DIR = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


class Workspace:
    def __init__(self, root: str = "/workspace") -> None:
        self.fd = os.open(root, _DIR)

    def close(self) -> None:
        os.close(self.fd)

    def _parts(self, path: str) -> list[str]:
        if not path or "\x00" in path or "\\" in path or len(path.encode()) > 4096:
            raise ValueError("Invalid workspace path")
        if path.startswith("/"):
            if path != "/workspace" and not path.startswith("/workspace/"):
                raise ValueError("Path must be inside /workspace")
            path = path[len("/workspace") :]
        parts = path.split("/")
        if ".." in parts:
            raise ValueError("Path traversal is not allowed")
        return [part for part in parts if part not in ("", ".")]

    @contextmanager
    def _directory(self, parts: list[str], *, create: bool = False) -> Iterator[int]:
        fd = os.dup(self.fd)
        try:
            for part in parts:
                if create:
                    try:
                        os.mkdir(part, mode=0o700, dir_fd=fd)
                    except FileExistsError:
                        pass
                child = os.open(part, _DIR, dir_fd=fd)
                os.close(fd)
                fd = child
            yield fd
        finally:
            os.close(fd)

    def read(self, path: str) -> FileResult:
        try:
            parts = self._parts(path)
            if not parts:
                raise ValueError("Path is a directory")
            with self._directory(parts[:-1]) as parent:
                fd = os.open(
                    parts[-1],
                    os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                    dir_fd=parent,
                )
                with os.fdopen(fd, "rb") as stream:
                    if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                        raise ValueError("Only regular text files are supported")
                    raw = stream.read(512 * 1024 + 1)
            truncated = len(raw) > 512 * 1024
            text = raw[: 512 * 1024].decode("utf-8", errors="replace")
            if truncated:
                text += "\n[truncated at 512 KiB]"
            return FileResult(content=text, truncated=truncated)
        except (OSError, ValueError) as exc:
            return FileResult(
                content=f"Cannot read {path}: {exc}", is_error=True, error_type="file_error"
            )

    def write(self, path: str, content: str) -> FileResult:
        try:
            raw = content.encode("utf-8")
            if len(raw) > 1024 * 1024:
                raise ValueError("Content exceeds 1 MiB")
            parts = self._parts(path)
            if not parts:
                raise ValueError("Path is a directory")
            with self._directory(parts[:-1], create=True) as parent:
                # Do not truncate before fstat: a FIFO/device must never be opened
                # with blocking I/O or treated as a regular text file.
                fd = os.open(
                    parts[-1],
                    os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                    0o600,
                    dir_fd=parent,
                )
                with os.fdopen(fd, "wb") as stream:
                    if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                        raise ValueError("Only regular text files are supported")
                    os.ftruncate(stream.fileno(), 0)
                    stream.write(raw)
            return FileResult(content=f"Wrote {len(raw)} bytes to {path}")
        except (OSError, ValueError) as exc:
            return FileResult(
                content=f"Cannot write {path}: {exc}", is_error=True, error_type="file_error"
            )

    def list(self, path: str, max_depth: int) -> ListResult:
        try:
            lines: list[str] = []
            truncated = False

            def visit(fd: int, depth: int) -> None:
                nonlocal truncated
                # Bound collection itself, not only the final display. Read up to
                # 201 entries then sort that bounded set; huge dirs are truncated.
                with os.scandir(fd) as scan:
                    names = []
                    for entry in scan:
                        names.append(entry.name)
                        if len(names) > 200:
                            truncated = True
                            break
                for name in sorted(names[:200]):
                    if len(lines) >= 200:
                        truncated = True
                        break
                    info = os.stat(name, dir_fd=fd, follow_symlinks=False)
                    is_dir = stat.S_ISDIR(info.st_mode)
                    display = os.fsencode(name).decode("utf-8", errors="replace")
                    lines.append("  " * depth + display + ("/" if is_dir else ""))
                    if is_dir and depth + 1 < max_depth:
                        child = os.open(name, _DIR, dir_fd=fd)
                        try:
                            visit(child, depth + 1)
                        finally:
                            os.close(child)

            with self._directory(self._parts(path)) as fd:
                visit(fd, 0)
            content = "\n".join(lines) or "[empty directory]"
            if truncated:
                content += "\n[truncated at 200 entries]"
            return ListResult(content=content, truncated=truncated)
        except (OSError, ValueError) as exc:
            return ListResult(
                content=f"Cannot list {path}: {exc}", is_error=True, error_type="file_error"
            )
