from typing import Any

import flyte

from flyte_mcp.models import DiscoveredTask
from flyte_mcp.settings import get_settings


def get_execution_project(
    project: str | None = None, task: DiscoveredTask | None = None
) -> str | None:
    # The precedence for determining the execution project is as follows:
    # 1. The `project` argument provided to the function.
    # 2. The `execution_project` specified in the settings.
    # 3. The `project` attribute of the provided `task`, if available.
    return (
        project or get_settings().execution_project or (task.project if task else None)
    )


def get_execution_domain(
    domain: str | None = None, task: DiscoveredTask | None = None
) -> str | None:
    # The precedence for determining the execution domain is as follows:
    # 1. The `domain` argument provided to the function.
    # 2. The `execution_domain` specified in the settings.
    # 3. The `domain` attribute of the provided `task`, if available.
    return domain or get_settings().execution_domain or (task.domain if task else None)


async def ensure_flyte_initialized() -> None:
    settings = get_settings()
    await flyte.init_in_cluster.aio(
        org=settings.org,
        project=settings.deployment_project,
        domain=settings.deployment_domain,
        endpoint=settings.endpoint,
    )


async def run_remote_task(
    task: Any,
    *,
    execution_project: str | None = None,
    execution_domain: str | None = None,
    overwrite_cache: bool = False,
    **inputs: Any,
) -> Any:
    return await flyte.with_runcontext(
        project=execution_project or get_settings().execution_project,
        domain=execution_domain or get_settings().execution_domain,
        overwrite_cache=overwrite_cache,
    ).run.aio(task, **inputs)
