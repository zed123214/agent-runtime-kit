"""Bounded, explicitly committed result of one chat turn."""

from typing import Literal

from pydantic import BaseModel


class RunGetResultResult(BaseModel):
    session_id: str
    run_id: str
    commit_state: Literal["pending", "committed", "unavailable"]
    result_revision: str | None = None
    transcript_ref: str | None = None
    status: Literal["success", "failed"] | None = None
    reason: str | None = None
    final_answer: str | None = None
    no_final_answer: bool = True
    truncated: bool = False
