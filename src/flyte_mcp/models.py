import enum
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from flyte.models import NativeInterface
from pydantic import BaseModel, ConfigDict, Field


class StructuredDatasetInput(BaseModel):
    uri: str
    format: str = Field(default="")
    hash: str | None = None


class TaskIdentifier(BaseModel):
    project: str
    domain: str
    version: str
    task_name: str


class RenderedTaskInput(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    required: bool
    description: str
    input_schema: dict[str, object] | None = Field(default=None, alias="schema")
    flyte_input_schema: dict[str, object] | None = Field(
        default=None, alias="flyte_schema"
    )


class RenderedTaskOutput(BaseModel):
    name: str
    required: bool
    description: str


class RenderedTask(BaseModel):
    task_identifier: TaskIdentifier
    tool_name: str
    environment: str | None = None
    description: str
    inputs: list[RenderedTaskInput]
    outputs: list[RenderedTaskOutput]
    defaults: dict[str, object] = Field(default_factory=dict)


@dataclass(frozen=True)
class DiscoveredTask:
    tool_name: str
    task_name: str
    project: str
    domain: str
    version: str
    environment: str | None
    description: str
    native_interface: NativeInterface
    defaults: dict[str, Any]
    input_descriptions: dict[str, str]
    output_descriptions: dict[str, str]
    input_type_metadata: dict[str, dict[str, Any]]
    output_type_metadata: dict[str, dict[str, Any]]


@dataclass(frozen=True)
class SkippedTask:
    task_name: str
    project: str
    domain: str
    environment: str | None
    reason: str


def _phase_name(phase: Any) -> str:
    if hasattr(phase, "name"):
        return str(phase.name).lower()
    rendered = str(phase)
    if "." in rendered:
        rendered = rendered.rsplit(".", 1)[-1]
    return rendered.lower()


def to_mcp_payload(value: Any) -> Any:
    if hasattr(value, "named_outputs"):
        return {key: to_mcp_payload(item) for key, item in value.named_outputs.items()}

    if isinstance(value, enum.Enum):
        return value.value

    if isinstance(value, datetime | date):
        return value.isoformat()

    if isinstance(value, Path):
        return str(value)

    if is_dataclass(value) and not isinstance(value, type):
        return {key: to_mcp_payload(item) for key, item in asdict(value).items()}

    if isinstance(value, BaseModel):
        return to_mcp_payload(value.model_dump(exclude_none=True))

    if isinstance(value, dict):
        return {str(key): to_mcp_payload(item) for key, item in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [to_mcp_payload(item) for item in value]

    if hasattr(value, "tolist") and callable(value.tolist):
        return to_mcp_payload(value.tolist())

    if isinstance(value, str | int | float | bool) or value is None:
        return value

    if hasattr(value, "to_dict") and callable(value.to_dict):
        return to_mcp_payload(value.to_dict())

    if hasattr(value, "__dict__"):
        return {
            str(key): to_mcp_payload(item)
            for key, item in vars(value).items()
            if not key.startswith("_")
        }

    return str(value)


def run_status_payload(run: Any, details: Any | None = None) -> dict[str, Any]:
    action_details = (
        getattr(details, "action_details", None) if details is not None else None
    )
    phase = getattr(run, "phase", None)
    done = run.done() if hasattr(run, "done") else False

    payload = {
        "run_name": getattr(run, "name", None),
        "phase": _phase_name(phase),
        "done": done,
        "success": _phase_name(phase) == "succeeded",
        "url": getattr(run, "url", None),
        "task_name": getattr(
            action_details, "task_name", getattr(run, "task_name", None)
        ),
        "error": None,
    }

    error_info = getattr(action_details, "error_info", None)
    if error_info is not None:
        payload["error"] = getattr(error_info, "message", str(error_info))

    return payload
