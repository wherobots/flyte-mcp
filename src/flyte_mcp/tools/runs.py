from collections.abc import Sequence
from typing import Annotated, Any, Literal, cast

from pydantic import Field

from flyte.remote import Run, TimeFilter
from flyte.remote._run import RunDetails
from flyte._initialize import get_init_config
from flyteidl2.common import identifier_pb2
from flyteidl2.workflow import run_definition_pb2
from flyte_mcp.models import run_status_payload, to_mcp_payload
from flyte_mcp.runtime import get_execution_domain, get_execution_project

SortDirection = Literal["asc", "desc"]
SortByParam = Annotated[list[str], Field(min_length=2, max_length=2)]


def _normalize_in_phase(
    in_phase: list[str] | tuple[str, ...] | None,
) -> tuple[str, ...] | None:
    if in_phase is None:
        return None

    normalized = tuple(
        phase.strip().lower()
        for phase in in_phase
        if isinstance(phase, str) and phase.strip()
    )
    return normalized or None


def _normalize_sort_by(sort_by: Sequence[str] | None) -> tuple[str, SortDirection]:
    if sort_by is None:
        return ("created_at", "desc")

    if len(sort_by) != 2:
        raise ValueError("sort_by must be [field, direction]")

    field, direction = sort_by
    normalized_direction = direction.strip().lower()
    if normalized_direction not in {"asc", "desc"}:
        raise ValueError("sort_by direction must be 'asc' or 'desc'")

    return (field, cast(SortDirection, normalized_direction))


async def _get_run(
    run_name: str,
    *,
    project: str | None = None,
    domain: str | None = None,
) -> Run:
    if project is None and domain is None:
        return await Run.get.aio(name=run_name)

    resolved_project = get_execution_project(project)
    resolved_domain = get_execution_domain(domain)
    cfg = get_init_config()
    details = await RunDetails.get_details.aio(
        identifier_pb2.RunIdentifier(
            org=cfg.org,
            project=resolved_project,
            domain=resolved_domain,
            name=run_name,
        )
    )
    run = run_definition_pb2.Run(
        action=run_definition_pb2.Action(
            id=details.action_id,
            metadata=details.action_details.pb2.metadata,
            status=details.action_details.pb2.status,
        )
    )
    return Run(pb2=run, _details=details)


async def _get_run_status_payload(
    run_name: str,
    *,
    project: str | None = None,
    domain: str | None = None,
    include_details: bool = False,
) -> tuple[Run, dict[str, Any]]:
    run = await _get_run(run_name, project=project, domain=domain)
    details = await run.details.aio() if include_details else None
    return run, run_status_payload(run, details)


async def list_runs(
    limit: int = 10,
    in_phase: list[str] | None = None,
    task_name: str | None = None,
    task_version: str | None = None,
    created_by_subject: str | None = None,
    sort_by: SortByParam | None = None,
    project: str | None = None,
    domain: str | None = None,
    created_at: TimeFilter | None = None,
    updated_at: TimeFilter | None = None,
) -> dict[str, Any]:
    """
    List recent Flyte runs, optionally filtered by project, domain, task name, or phase.

    Parameters
    ----------
    limit : int, default=10
        Maximum number of runs to return.
    in_phase : list of str or None, default=None
        Optional phase filters such as ``queued``, ``running``, or ``succeeded``.
    task_name : str or None, default=None
        Optional fully qualified Flyte task name to filter by.
    task_version : str or None, default=None
        Optional Flyte task version to filter by.
    created_by_subject : str or None, default=None
        Optional creator subject to filter by.
    sort_by : list[str] or None, default=None
        Optional two-item sort array ``[field, direction]``. Defaults to
        ``["created_at", "desc"]`` when omitted.
    project : str or None, default=None
        Optional Flyte project override. Falls back to the configured execution
        project when omitted.
    domain : str or None, default=None
        Optional Flyte domain override. Falls back to the configured execution
        domain when omitted.
    created_at : TimeFilter or None, default=None
        Optional created-at time range filter.
    updated_at : TimeFilter or None, default=None
        Optional updated-at time range filter.

    Returns
    -------
    dict
        Payload containing the resolved scope, applied filters, run count, and
        serialized run summaries.
    """
    resolved_project = get_execution_project(project)
    resolved_domain = get_execution_domain(domain)
    normalized_in_phase = _normalize_in_phase(in_phase)
    resolved_sort_by = _normalize_sort_by(sort_by)
    runs: list[dict[str, Any]] = []

    async for run in Run.listall.aio(
        limit=limit,
        in_phase=normalized_in_phase,
        task_name=task_name,
        task_version=task_version,
        created_by_subject=created_by_subject,
        sort_by=resolved_sort_by,
        project=resolved_project,
        domain=resolved_domain,
        created_at=created_at,
        updated_at=updated_at,
    ):
        details = await run.details.aio()
        runs.append(run_status_payload(run, details))

    return {
        "project": resolved_project,
        "domain": resolved_domain,
        "limit": limit,
        "count": len(runs),
        "in_phase": list(normalized_in_phase) if normalized_in_phase else None,
        "task_name": task_name,
        "task_version": task_version,
        "created_by_subject": created_by_subject,
        "sort_by": to_mcp_payload(resolved_sort_by),
        "created_at": to_mcp_payload(created_at),
        "updated_at": to_mcp_payload(updated_at),
        "runs": runs,
    }


async def get_run_status(
    run_name: str,
    project: str | None = None,
    domain: str | None = None,
) -> dict[str, Any]:
    """
    Fetch the current status for a Flyte run by name.

    Parameters
    ----------
    run_name : str
        Flyte run identifier.
    project : str or None, default=None
        Optional Flyte project override for the run lookup.
    domain : str or None, default=None
        Optional Flyte domain override for the run lookup.

    Returns
    -------
    dict
        Serialized run status, including phase, success state, URL, and task
        metadata when available.
    """
    _, status = await _get_run_status_payload(
        run_name,
        project=project,
        domain=domain,
        include_details=True,
    )
    return status


async def get_run_inputs(
    run_name: str,
    project: str | None = None,
    domain: str | None = None,
) -> dict[str, Any]:
    """
    Fetch the resolved inputs for a Flyte run by name.

    Parameters
    ----------
    run_name : str
        Flyte run identifier.
    project : str or None, default=None
        Optional Flyte project override for the run lookup.
    domain : str or None, default=None
        Optional Flyte domain override for the run lookup.

    Returns
    -------
    dict
        Serialized run status augmented with the run input payload.
    """
    run, status = await _get_run_status_payload(
        run_name,
        project=project,
        domain=domain,
    )
    inputs = await run.inputs.aio()
    return {
        **status,
        "inputs": to_mcp_payload(inputs),
    }


async def get_run_outputs(
    run_name: str,
    wait: bool = False,
    project: str | None = None,
    domain: str | None = None,
) -> dict[str, Any]:
    """
    Fetch outputs for a Flyte run, optionally waiting for completion first.

    Parameters
    ----------
    run_name : str
        Flyte run identifier.
    wait : bool, default=False
        If ``True``, wait for the run to reach a terminal state before reading
        outputs.
    project : str or None, default=None
        Optional Flyte project override for the run lookup.
    domain : str or None, default=None
        Optional Flyte domain override for the run lookup.

    Returns
    -------
    dict
        Serialized run status plus either the resolved outputs or a not-ready
        message when the run is still in progress.
    """
    run = await _get_run(run_name, project=project, domain=domain)
    if wait:
        await run.wait.aio()

    status = run_status_payload(run)
    if not run.done():
        return {
            **status,
            "ready": False,
            "outputs": None,
            "message": "Outputs are not available until the run reaches a terminal state.",
        }

    outputs = await run.outputs.aio()
    return {
        **status,
        "ready": True,
        "outputs": to_mcp_payload(outputs),
    }


async def abort_run(
    run_name: str,
    reason: str = "Aborted via Dynamic Flyte MCP.",
    project: str | None = None,
    domain: str | None = None,
) -> dict[str, Any]:
    """
    Request cancellation of a Flyte run and return the refreshed run status.

    Parameters
    ----------
    run_name : str
        Flyte run identifier.
    reason : str, default="Aborted via Dynamic Flyte MCP."
        Human-readable cancellation reason sent to Flyte.
    project : str or None, default=None
        Optional Flyte project override for the run lookup.
    domain : str or None, default=None
        Optional Flyte domain override for the run lookup.

    Returns
    -------
    dict
        Refreshed serialized run status including cancellation metadata.
    """
    run = await _get_run(run_name, project=project, domain=domain)
    await run.abort.aio(reason=reason)
    _, status = await _get_run_status_payload(
        run_name,
        project=project,
        domain=domain,
        include_details=True,
    )
    return {
        **status,
        "abort_requested": True,
        "abort_reason": reason,
    }


RUN_TOOLS = [
    list_runs,
    get_run_status,
    get_run_inputs,
    get_run_outputs,
    abort_run,
]
