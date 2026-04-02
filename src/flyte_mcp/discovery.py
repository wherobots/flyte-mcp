import asyncio
import enum
import logging
from functools import reduce
import operator
from types import UnionType
from typing import Annotated, Any, Literal, Union, get_args, get_origin

import flyte
import flyte.errors
from flyte.io import DataFrame, File
from flyte.remote import Task, TaskDetails
from flyte.types import TypeEngine
from flyteidl2.core import literals_pb2
from google.protobuf.json_format import MessageToDict
from pydantic import Field, TypeAdapter

from flyte_mcp.models import (
    DiscoveredTask,
    RenderedTask,
    RenderedTaskInput,
    RenderedTaskOutput,
    SkippedTask,
    StructuredDatasetInput,
    TaskIdentifier,
    to_mcp_payload,
)
from flyte_mcp.settings import DiscoveryProject, get_settings

logger = logging.getLogger(__name__)


def task_description(task: TaskDetails) -> str | None:
    short_description = str(task.pb2.spec.documentation.short_description).strip()
    long_description = str(task.pb2.spec.documentation.long_description).strip()
    if short_description and long_description:
        return f"{short_description}\n\n{long_description}"
    return short_description or long_description or None


def _python_class_name(type_metadata: dict[str, Any] | None) -> str | None:
    if not type_metadata:
        return None
    value = type_metadata.get("python_class_name")
    return str(value) if value else None


def _mcp_annotation(
    python_type: Any, type_metadata: dict[str, Any] | None = None
) -> Any:
    python_class_name = _python_class_name(type_metadata)
    origin = get_origin(python_type)
    if origin in {Union, UnionType}:
        variants = [
            _mcp_annotation(item, type_metadata) for item in get_args(python_type)
        ]
        if not variants:
            return object
        if len(variants) == 1:
            return variants[0]
        return reduce(operator.or_, variants[1:], variants[0])

    if origin is list:
        args = get_args(python_type)
        return list[_mcp_annotation(args[0], type_metadata)] if args else list[Any]

    if origin is dict:
        key_type, value_type = (get_args(python_type) + (str, Any))[:2]
        return dict[key_type, _mcp_annotation(value_type, type_metadata)]

    if isinstance(python_type, type) and issubclass(python_type, enum.Enum):
        values = tuple(member.value for member in python_type)
        if values:
            return Literal.__getitem__(values)
        return str

    if python_type is File:
        return str | File

    if python_type is DataFrame:
        return str | StructuredDatasetInput

    if isinstance(python_type, type) and any(
        base.__module__ == "flyte.types._pickle" and base.__name__ == "FlytePickle"
        for base in python_type.__mro__
    ):
        if python_class_name == "<class 'numpy.ndarray'>":
            return list[Any]
        return object

    return python_type


def _variable_payloads(task: TaskDetails, direction: str) -> dict[str, dict[str, Any]]:
    interface = (
        task.pb2.spec.task_template.interface.inputs
        if direction == "inputs"
        else task.pb2.spec.task_template.interface.outputs
    )
    return {entry.key: MessageToDict(entry.value) for entry in interface.variables}


def _type_metadata(
    variables: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {
        name: dict(type_metadata)
        for name, variable in variables.items()
        if isinstance(
            (type_metadata := variable.get("type", {}).get("metadata", {})), dict
        )
        and type_metadata
    }


async def _defaults_from_task(task: TaskDetails) -> dict[str, Any]:
    remote_defaults = task.interface._remote_defaults or {}
    if not remote_defaults:
        return {}

    input_types = {
        name: python_type
        for name, python_type in task.interface.get_input_types().items()
        if name in remote_defaults
    }
    defaults = await TypeEngine.literal_map_to_kwargs(
        literals_pb2.LiteralMap(literals=remote_defaults),
        python_types=input_types,
    )
    return defaults


def _annotate(annotation: Any, description: str) -> Any:
    if not description:
        return annotation
    return Annotated[annotation, Field(description=description)]


async def build_discovered_task(
    task: TaskDetails,
    *,
    require_documentation: bool = True,
) -> DiscoveredTask | SkippedTask:
    task_name = task.name
    project = task.pb2.task_id.project
    domain = task.pb2.task_id.domain
    version = task.version
    environment = str(task.pb2.spec.environment.name) or None
    description = task_description(task)

    if not description and require_documentation:
        return SkippedTask(
            task_name=task_name,
            project=project,
            domain=domain,
            environment=environment,
            reason="missing documentation.shortDescription",
        )

    input_variables = _variable_payloads(task, "inputs")
    output_variables = _variable_payloads(task, "outputs")
    defaults = await _defaults_from_task(task)
    native_interface = task.interface

    return DiscoveredTask(
        tool_name=task_name,
        task_name=task_name,
        project=project,
        domain=domain,
        version=version,
        environment=environment,
        description=description or task_name,
        native_interface=native_interface,
        defaults=defaults,
        input_descriptions={
            name: str(variable.get("description", "")).strip()
            for name, variable in input_variables.items()
        },
        output_descriptions={
            name: str(variable.get("description", "")).strip()
            for name, variable in output_variables.items()
        },
        input_type_metadata=_type_metadata(input_variables),
        output_type_metadata=_type_metadata(output_variables),
    )


async def _list_task_names(
    project: str,
    domain: str,
    limit: int,
) -> list[str]:
    names: set[str] = set()
    async for task in Task.listall.aio(project=project, domain=domain, limit=limit):
        names.add(task.name)
    return sorted(names)


async def fetch_task_details(
    *,
    project: str,
    domain: str,
    task_name: str,
    version: str = "latest",
) -> TaskDetails:
    get_kwargs: dict[str, Any] = {
        "project": project,
        "domain": domain,
        "name": task_name,
    }
    if version == "latest":
        get_kwargs["auto_version"] = "latest"
    else:
        get_kwargs["version"] = version
    task = Task.get(**get_kwargs)
    return await task.fetch.aio()


async def list_project_versions(
    *,
    project: str,
    domain: str | None = None,
    limit: int | None = None,
) -> list[str]:
    settings = get_settings()
    domain = domain or settings.discovery_domain
    limit = limit or settings.discovery_limit
    versions: list[str] = []
    seen: set[str] = set()

    async for task in Task.listall.aio(
        project=project,
        domain=domain,
        sort_by=("created_at", "desc"),
        limit=limit,
    ):
        if task.version in seen:
            continue
        seen.add(task.version)
        versions.append(task.version)
    return versions


async def list_task_versions(
    *,
    project: str,
    domain: str | None = None,
    task_name: str,
    limit: int | None = None,
) -> list[str]:
    settings = get_settings()
    domain = domain or settings.discovery_domain
    limit = limit or settings.discovery_limit
    versions: list[str] = []
    seen: set[str] = set()

    async for task in Task.listall.aio(
        by_task_name=task_name,
        project=project,
        domain=domain,
        sort_by=("created_at", "desc"),
        limit=limit,
    ):
        if task.version in seen:
            continue
        seen.add(task.version)
        versions.append(task.version)
    return versions


async def discover_tasks(
    *,
    projects: tuple[DiscoveryProject, ...] | None = None,
    domain: str | None = None,
    limit: int | None = None,
    concurrency: int | None = None,
) -> tuple[list[DiscoveredTask], list[SkippedTask]]:
    if any(value is None for value in (projects, domain, limit, concurrency)):
        settings = get_settings()
        projects = settings.discovery_projects if projects is None else projects
        domain = settings.discovery_domain if domain is None else domain
        limit = settings.discovery_limit if limit is None else limit
        concurrency = (
            settings.discovery_concurrency if concurrency is None else concurrency
        )

    semaphore = asyncio.Semaphore(concurrency)
    discovered: list[DiscoveredTask] = []
    skipped: list[SkippedTask] = []

    async def _fetch(project: DiscoveryProject, task_name: str) -> TaskDetails:
        async with semaphore:
            return await fetch_task_details(
                project=project.project,
                domain=domain,
                task_name=task_name,
                version=project.version,
            )

    fetches: list[asyncio.Future[Any] | asyncio.Task[Any]] = []
    fetch_order: list[tuple[DiscoveryProject, str]] = []

    for project in projects:
        task_names = await _list_task_names(project.project, domain, limit)
        if not task_names:
            logger.warning(
                "No tasks discovered for project=%s domain=%s version=%s",
                project.project,
                domain,
                project.version,
            )
        for task_name in task_names:
            fetch_order.append((project, task_name))
            fetches.append(asyncio.create_task(_fetch(project, task_name)))

    for (project, task_name), result in zip(
        fetch_order,
        await asyncio.gather(*fetches, return_exceptions=True),
        strict=False,
    ):
        if isinstance(result, flyte.errors.RemoteTaskNotFoundError):
            skipped.append(
                SkippedTask(
                    task_name=task_name,
                    project=project.project,
                    domain=domain,
                    environment=task_name.split(".", 1)[0]
                    if "." in task_name
                    else None,
                    reason=f"task version {project.version} not found",
                )
            )
            continue
        if isinstance(result, Exception):
            raise result
        task_or_skip = await build_discovered_task(result)
        if isinstance(task_or_skip, SkippedTask):
            skipped.append(task_or_skip)
        else:
            discovered.append(task_or_skip)

    return discovered, skipped


def mcp_input_annotation(task: DiscoveredTask, name: str) -> Any:
    python_type = task.native_interface.get_input_types()[name]
    return _annotate(
        _mcp_annotation(
            python_type,
            task.input_type_metadata.get(name),
        ),
        task.input_descriptions.get(name, ""),
    )


def render_task(task: DiscoveredTask) -> RenderedTask:
    flyte_input_schema = task.native_interface.json_schema
    required_inputs = set(task.native_interface.required_inputs())

    return RenderedTask(
        task_identifier=TaskIdentifier(
            project=task.project,
            domain=task.domain,
            version=task.version,
            task_name=task.task_name,
        ),
        tool_name=task.tool_name,
        environment=task.environment,
        description=task.description,
        inputs=[
            RenderedTaskInput(
                name=name,
                required=name in required_inputs,
                description=task.input_descriptions.get(name, ""),
                input_schema=TypeAdapter(
                    mcp_input_annotation(task, name)
                ).json_schema(),
                flyte_input_schema=flyte_input_schema.get("properties", {}).get(name),
            )
            for name in task.native_interface.inputs
        ],
        outputs=[
            RenderedTaskOutput(
                name=name,
                required=False,
                description=task.output_descriptions.get(name, ""),
            )
            for name in task.native_interface.outputs
        ],
        defaults=to_mcp_payload(task.defaults),
    )
