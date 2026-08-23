from __future__ import annotations

import re
from typing import get_args

from pydantic import BaseModel
from scripts.generate_wire_protocol import generate

from agent_runtime.core.bus import commands, events


def _runtime_command_model_names() -> set[str]:
    command_union = get_args(commands.Command)[0]
    return {model.__name__ for model in get_args(command_union)}


def _declared_result_model_names() -> set[str]:
    return {
        name
        for name, model in vars(commands).items()
        if name.endswith("Result") and isinstance(model, type) and issubclass(model, BaseModel)
    }


def _runtime_event_model_names() -> set[str]:
    event_union = get_args(events.Event)[0]
    return {model.__name__ for model in get_args(event_union)}


def _documented_model_names(document: str, suffix: str) -> set[str]:
    return set(re.findall(rf"^### (\w+{suffix})$", document, flags=re.MULTILINE))


def test_all_runtime_commands_are_documented_once() -> None:
    document = generate()
    expected = _runtime_command_model_names()

    assert _documented_model_names(document, "Command") == expected
    assert all(document.count(f"### {name}\n") == 1 for name in expected)


def test_all_declared_command_results_are_documented_once() -> None:
    document = generate()
    expected = _declared_result_model_names()

    assert _documented_model_names(document, "Result") == expected
    assert all(document.count(f"### {name}\n") == 1 for name in expected)


def test_all_runtime_events_are_documented_once() -> None:
    document = generate()
    expected = _runtime_event_model_names()

    assert _documented_model_names(document, "Event") == expected
    assert all(document.count(f"### {name}\n") == 1 for name in expected)
