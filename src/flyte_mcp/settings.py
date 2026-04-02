from functools import lru_cache
from pathlib import Path

import yaml
from pydantic import BaseModel


class DiscoveryProject(BaseModel):
    project: str
    version: str = "latest"


class MCPSettings(BaseModel):
    """Configuration for the Flyte MCP server, loaded from ``deploy.yaml``.

    Attributes
    ----------
    org : str or None
        Organization slug on the Flyte platform. Required for in-cluster
        initialization.
    endpoint : str or None
        gRPC endpoint for the Flyte platform, e.g. ``dns:///<hostname>``.
        Required for in-cluster initialization.
    deployment_project : str
        Flyte project that owns this MCP app environment. Used when the server
        initializes itself via ``flyte.init_in_cluster``.
    deployment_domain : str
        Flyte domain for the MCP app environment (e.g. ``staging``).
    execution_project : str
        Default Flyte project used when launching task runs via the MCP tools.
        Can be overridden per ``run_task`` call; falls back to the task's own
        project if neither is supplied.
    execution_domain : str
        Default Flyte domain used when launching task runs via the MCP tools.
        Can be overridden per ``run_task`` call; falls back to the task's own
        domain if neither is supplied.
    discovery_projects : tuple of DiscoveryProject
        Project/version pairs the server indexes when a client calls
        ``discover_tasks`` or ``list_project_versions``. Use version
        ``"latest"`` to always resolve the most recently registered version.
    discovery_domain : str
        Flyte domain used during task discovery. Defaults to ``"development"``.
    discovery_limit : int
        Maximum number of task rows scanned per project during discovery.
        Increase for projects with large numbers of registered tasks.
        Defaults to ``512``.
    discovery_concurrency : int
        Number of tasks fetched concurrently during discovery. Defaults to
        ``32``.
    """

    org: str | None = None
    endpoint: str | None = None
    deployment_project: str
    deployment_domain: str
    execution_project: str
    execution_domain: str
    discovery_projects: tuple[DiscoveryProject, ...] = ()
    discovery_domain: str = "development"
    discovery_limit: int = 512
    discovery_concurrency: int = 32


def _find_config() -> Path:
    for candidate in (
        Path.cwd() / "deploy.yaml",
        Path(__file__).parent.parent.parent / "deploy.yaml",
    ):
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "deploy.yaml not found in current directory or project root"
    )


@lru_cache(maxsize=1)
def get_settings() -> MCPSettings:
    data = yaml.safe_load(_find_config().read_text())
    return MCPSettings.model_validate(data)
