from types import UnionType
from typing import Any, Callable, Union, get_args, get_origin

from flyte.io import DataFrame, File
from flyte.remote import Task
from pydantic import TypeAdapter, ValidationError

from flyte_mcp.discovery import (
    build_discovered_task,
    discover_tasks as discover_remote_tasks,
    fetch_task_details,
    list_project_versions as discover_project_versions,
    list_task_versions as discover_task_versions,
    mcp_input_annotation,
    render_task,
)
from flyte_mcp.models import (
    DiscoveredTask,
    SkippedTask,
    StructuredDatasetInput,
    TaskIdentifier,
    run_status_payload,
    to_mcp_payload,
)
from flyte_mcp.runtime import (
    get_execution_domain,
    get_execution_project,
    run_remote_task,
)
from flyte_mcp.settings import DiscoveryProject, get_settings

InputCoercer = Callable[[Any], Any]


def _normalize_discovery_projects(
    projects: list[DiscoveryProject] | tuple[DiscoveryProject, ...] | None,
) -> tuple[DiscoveryProject, ...] | None:
    if projects is None:
        return None

    by_project: dict[str, DiscoveryProject] = {}
    for project in projects:
        existing = by_project.get(project.project)
        if existing is not None and existing.version != project.version:
            raise ValueError(
                f"Duplicate discovery project '{project.project}' with conflicting versions"
            )
        by_project[project.project] = project
    return tuple(sorted(by_project.values(), key=lambda item: item.project))


def _resolve_task_selector(
    *,
    task_identifier: TaskIdentifier | None,
    project: str | None,
    task_name: str | None,
    version: str | None,
    domain: str | None,
) -> tuple[str, str, str, str]:
    if task_identifier is not None:
        return (
            task_identifier.project,
            task_identifier.domain,
            task_identifier.version,
            task_identifier.task_name,
        )

    if not project:
        raise ValueError("project is required when task_identifier is not provided")
    if not task_name:
        raise ValueError("task_name is required when task_identifier is not provided")
    if not version:
        raise ValueError("version is required when task_identifier is not provided")

    return project, domain or get_settings().discovery_domain, version, task_name


def _python_class_name(type_metadata: dict[str, Any]) -> str | None:
    value = type_metadata.get("python_class_name")
    return str(value) if value else None


def _coerce_dataframe_input(value: Any) -> Any:
    if isinstance(value, str):
        return DataFrame(uri=value)
    if isinstance(value, StructuredDatasetInput):
        return DataFrame(uri=value.uri, format=value.format, hash=value.hash)
    if isinstance(value, dict) and "uri" in value:
        return DataFrame.model_validate(value)
    return value


def _coerce_file_input(value: Any) -> Any:
    if isinstance(value, str):
        return File(path=value)
    return value


def _coerce_numpy_input(value: Any) -> Any:
    try:
        import numpy as np

        if isinstance(value, list | tuple):
            return np.array(value)
    except ImportError:
        return value
    return value


_TYPE_INPUT_COERCERS: dict[type[Any], InputCoercer] = {
    DataFrame: _coerce_dataframe_input,
    File: _coerce_file_input,
}

_METADATA_INPUT_COERCERS: dict[str, InputCoercer] = {
    "<class 'numpy.ndarray'>": _coerce_numpy_input,
}


def _input_coercer_for_expected_type(expected_type: Any) -> InputCoercer | None:
    origin = get_origin(expected_type)
    if origin in {Union, UnionType}:
        matches: list[InputCoercer] = []
        for arg in get_args(expected_type):
            if arg is type(None):
                continue
            coercer = _input_coercer_for_expected_type(arg)
            if coercer is not None and coercer not in matches:
                matches.append(coercer)
        return matches[0] if len(matches) == 1 else None
    return _TYPE_INPUT_COERCERS.get(expected_type)


def _coerce_mcp_value_to_flyte_input(
    expected_type: Any, type_metadata: dict[str, Any] | None, value: Any
) -> Any:
    coercer = _input_coercer_for_expected_type(expected_type)
    if coercer is not None:
        return coercer(value)

    python_class_name = _python_class_name(type_metadata or {})
    if python_class_name is not None:
        coercer = _METADATA_INPUT_COERCERS.get(python_class_name)
        if coercer is not None:
            return coercer(value)
    return value


def _validate_against_annotation(
    task: DiscoveredTask, parameter_name: str, value: Any
) -> Any:
    annotation = mcp_input_annotation(task, parameter_name)
    try:
        return TypeAdapter(annotation).validate_python(value)
    except ValidationError as exc:
        messages = []
        for error in exc.errors(include_url=False):
            location = ".".join(str(part) for part in error.get("loc", ()))
            detail = str(error.get("msg", "invalid value"))
            if location:
                messages.append(f"{location}: {detail}")
            else:
                messages.append(detail)
        rendered = "; ".join(messages) or str(exc)
        raise ValueError(f"Invalid input for '{parameter_name}': {rendered}") from exc


def _coerce_arguments(
    task: DiscoveredTask, raw_arguments: dict[str, Any]
) -> dict[str, Any]:
    input_types = task.native_interface.get_input_types()
    coerced: dict[str, Any] = {}
    for parameter_name in task.native_interface.inputs:
        if parameter_name not in raw_arguments:
            continue
        validated_value = _validate_against_annotation(
            task,
            parameter_name,
            raw_arguments[parameter_name],
        )
        coerced[parameter_name] = _coerce_mcp_value_to_flyte_input(
            input_types.get(parameter_name, object),
            task.input_type_metadata.get(parameter_name),
            validated_value,
        )
    return coerced


async def _resolved_task(
    *,
    project: str,
    task_name: str,
    version: str,
    domain: str,
    require_documentation: bool,
) -> DiscoveredTask:
    task_details = await fetch_task_details(
        project=project,
        domain=domain,
        task_name=task_name,
        version=version,
    )
    task = await build_discovered_task(
        task_details,
        require_documentation=require_documentation,
    )
    if isinstance(task, SkippedTask):
        raise ValueError(f"Task '{task_name}' is unavailable: {task.reason}")
    return task


async def list_project_versions(
    projects: list[str] | None = None,
    domain: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """
    List available Flyte task versions for one or more projects.

    Parameters
    ----------
    projects : list of str or None, default=None
        Projects to inspect. When omitted, the configured discovery projects are used.
    domain : str or None, default=None
        Flyte domain to inspect. Falls back to the configured discovery domain.
    limit : int or None, default=None
        Maximum number of task rows to scan per project while collecting versions.

    Returns
    -------
    dict[str, Any]
        The resolved domain, requested projects, and discovered versions per project.
    """
    resolved_domain = domain or get_settings().discovery_domain
    resolved_projects = projects or [
        item.project for item in get_settings().discovery_projects
    ]
    payload = []
    for project in resolved_projects:
        payload.append(
            {
                "project": project,
                "versions": await discover_project_versions(
                    project=project,
                    domain=resolved_domain,
                    limit=limit,
                ),
            }
        )
    return {
        "domain": resolved_domain,
        "projects": payload,
    }


async def list_discovery_projects() -> dict[str, Any]:
    """
    List the configured default discovery project/version pairs.

    Call this before discovery when you need the server's default scope.
    Remember the returned project/version pairs and reuse them for later
    discovery and version inspection unless the user asks for a different
    project set.

    Returns
    -------
    dict[str, Any]
        The configured discovery domain and project/version pairs.
    """
    settings = get_settings()
    return {
        "domain": settings.discovery_domain,
        "count": len(settings.discovery_projects),
        "projects": [item.model_dump() for item in settings.discovery_projects],
    }


async def list_task_versions(
    project: str,
    task_name: str,
    domain: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """
    List available versions for one Flyte task.

    Parameters
    ----------
    project : str
        Flyte project containing the task.
    task_name : str
        Fully qualified Flyte task name.
    domain : str or None, default=None
        Flyte domain to inspect. Falls back to the configured discovery domain.
    limit : int or None, default=None
        Maximum number of task rows to scan while collecting versions.

    Returns
    -------
    dict[str, Any]
        The resolved scope and discovered versions for the requested task.
    """
    resolved_domain = domain or get_settings().discovery_domain
    return {
        "project": project,
        "domain": resolved_domain,
        "task_name": task_name,
        "versions": await discover_task_versions(
            project=project,
            domain=resolved_domain,
            task_name=task_name,
            limit=limit,
        ),
    }


async def discover_tasks(
    projects: list[DiscoveryProject] | None = None,
    domain: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """
    Discover documented Flyte tasks for the requested project/version set.

    Parameters
    ----------
    projects : list[DiscoveryProject] or None, default=None
        Project/version pairs to inspect. When omitted, the configured discovery
        projects are used.
    domain : str or None, default=None
        Flyte domain to inspect. Falls back to the configured discovery domain.
    limit : int or None, default=None
        Maximum number of task rows to inspect per project.

    Returns
    -------
    dict[str, Any]
        Resolved discovery scope plus discovered and skipped task summaries.
    """
    resolved_projects = (
        _normalize_discovery_projects(projects) or get_settings().discovery_projects
    )
    resolved_domain = domain or get_settings().discovery_domain
    discovered, skipped = await discover_remote_tasks(
        projects=resolved_projects,
        domain=resolved_domain,
        limit=limit,
    )
    grouped_tasks: dict[tuple[str, str], list[dict[str, Any]]] = {}
    rendered_tasks: list[dict[str, Any]] = []
    for task in discovered:
        rendered = {
            "task_identifier": TaskIdentifier(
                project=task.project,
                domain=task.domain,
                version=task.version,
                task_name=task.task_name,
            ).model_dump(),
            "tool_name": task.tool_name,
            "task_name": task.task_name,
            "project": task.project,
            "domain": task.domain,
            "version": task.version,
            "environment": task.environment,
            "description": task.description,
        }
        rendered_tasks.append(rendered)
        grouped_tasks.setdefault((task.project, task.version), []).append(rendered)
    return {
        "domain": resolved_domain,
        "projects": [
            {"project": item.project, "version": item.version}
            for item in resolved_projects
        ],
        "count": len(discovered),
        "skipped_count": len(skipped),
        "task_groups": [
            {
                "project": project,
                "domain": resolved_domain,
                "version": version,
                "count": len(tasks),
                "tasks": tasks,
            }
            for (project, version), tasks in sorted(grouped_tasks.items())
        ],
        "tasks": rendered_tasks,
        "skipped": [
            {
                "task_name": task.task_name,
                "project": task.project,
                "domain": task.domain,
                "environment": task.environment,
                "reason": task.reason,
            }
            for task in skipped
        ],
    }


async def describe_task(
    project: str | None = None,
    task_name: str | None = None,
    version: str | None = "latest",
    domain: str | None = None,
    task_identifier: TaskIdentifier | None = None,
) -> dict[str, Any]:
    """
    Describe one Flyte task, including rendered input schemas and defaults.

    Parameters
    ----------
    project : str or None, default=None
        Flyte project containing the task. Required when ``task_identifier`` is omitted.
    task_name : str or None, default=None
        Fully qualified Flyte task name. Required when ``task_identifier`` is omitted.
    version : str or None, default="latest"
        Exact task version. Defaults to ``latest`` when ``task_identifier`` is omitted.
    domain : str or None, default=None
        Flyte domain containing the task. Falls back to the configured discovery
        domain when ``task_identifier`` is omitted.
    task_identifier : TaskIdentifier or None, default=None
        Structured task identifier returned by ``discover_tasks`` or ``describe_task``.

    Returns
    -------
    dict[str, Any]
        Rendered task metadata including descriptions, input schemas, output
        names, and serialized defaults.
    """
    resolved_project, resolved_domain, resolved_version, resolved_task_name = (
        _resolve_task_selector(
            task_identifier=task_identifier,
            project=project,
            task_name=task_name,
            version=version,
            domain=domain,
        )
    )
    task = await _resolved_task(
        project=resolved_project,
        task_name=resolved_task_name,
        version=resolved_version,
        domain=resolved_domain,
        require_documentation=False,
    )
    payload = render_task(task).model_dump(by_alias=True, exclude_none=True)
    if not task.defaults:
        payload.pop("defaults", None)
    return payload


async def run_task(
    inputs: dict[str, Any],
    project: str | None = None,
    task_name: str | None = None,
    version: str | None = None,
    domain: str | None = None,
    task_identifier: TaskIdentifier | None = None,
    execution_project: str | None = None,
    execution_domain: str | None = None,
    wait: bool = False,
) -> dict[str, Any]:
    """
    Run one Flyte task with explicit inputs.

    Parameters
    ----------
    inputs : dict[str, Any]
        MCP-friendly input payload to validate and coerce into Flyte runtime inputs.
    project : str or None, default=None
        Flyte project containing the task. Required when ``task_identifier`` is omitted.
    task_name : str or None, default=None
        Fully qualified Flyte task name. Required when ``task_identifier`` is omitted.
    version : str or None, default=None
        Exact task version. Required when ``task_identifier`` is omitted.
    domain : str or None, default=None
        Flyte domain containing the task. Falls back to the configured discovery
        domain when ``task_identifier`` is omitted.
    task_identifier : TaskIdentifier or None, default=None
        Structured task identifier returned by ``discover_tasks`` or ``describe_task``.
    execution_project : str or None, default=None
        Flyte execution project override for this run. When omitted, the configured
        execution project is used, falling back to the runtime default resolution.
    execution_domain : str or None, default=None
        Flyte execution domain override for this run. When omitted, the configured
        execution domain is used, falling back to the runtime default resolution.
    wait : bool, default=False
        When ``True``, wait for the run to complete and include outputs when available.

    Returns
    -------
    dict[str, Any]
        Serialized task execution metadata, run status, and optional outputs.
    """
    resolved_project, resolved_domain, resolved_version, resolved_task_name = (
        _resolve_task_selector(
            task_identifier=task_identifier,
            project=project,
            task_name=task_name,
            version=version,
            domain=domain,
        )
    )
    task = await _resolved_task(
        project=resolved_project,
        task_name=resolved_task_name,
        version=resolved_version,
        domain=resolved_domain,
        require_documentation=False,
    )
    coerced_inputs = _coerce_arguments(task, inputs)
    lazy_task = Task.get(
        project=task.project,
        domain=task.domain,
        name=task.task_name,
        version=task.version,
    )

    resolved_exec_project = get_execution_project(execution_project, task)
    resolved_exec_domain = get_execution_domain(execution_domain, task)
    run = await run_remote_task(
        lazy_task,
        execution_project=resolved_exec_project,
        execution_domain=resolved_exec_domain,
        **coerced_inputs,
    )
    payload = {
        "task": {
            "task_identifier": TaskIdentifier(
                project=task.project,
                domain=task.domain,
                version=task.version,
                task_name=task.task_name,
            ).model_dump(),
            "task_name": task.task_name,
            "project": task.project,
            "domain": task.domain,
            "version": task.version,
        },
        "run_scope": {
            "project": resolved_exec_project,
            "domain": resolved_exec_domain,
        },
        "inputs": to_mcp_payload(coerced_inputs),
        **run_status_payload(run),
    }
    if wait:
        await run.wait.aio()
        payload.update(run_status_payload(run))
        if run.done():
            payload["outputs"] = to_mcp_payload(await run.outputs.aio())
    return payload


TASK_TOOLS = [
    list_discovery_projects,
    list_project_versions,
    list_task_versions,
    discover_tasks,
    describe_task,
    run_task,
]
