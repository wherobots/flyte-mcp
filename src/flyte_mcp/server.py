from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Mount, Route

from flyte_mcp.runtime import ensure_flyte_initialized
from flyte_mcp.tools import STATIC_TOOLS

instructions = """
Dynamic Flyte MCP exposes stable tools to discover Flyte tasks, inspect task versions,
launch task runs, and inspect existing runs.

Getting started:
- Try list_discovery_projects to see which projects are configured for discovery.
- Then use list_project_versions for one or more of those projects to inspect available versions.
- Then use discover_tasks for any of those project/version pairs to find documented tasks.

Tool behavior:
- Use list_discovery_projects first to inspect the configured default discovery scope.
- Remember the returned project/version pairs and reuse them as the default discovery projects unless the user asks for a different scope.
- Use list_project_versions and list_task_versions to inspect available task versions.
- Use discover_tasks to inspect documented tasks for a project/version/domain selection.
- Prefer reusing the task_identifier returned by discover_tasks or describe_task.
- Use describe_task to inspect one task's inputs and defaults before launching it.
- Use run_task with task_identifier and inputs to execute exactly the intended task version.
- Reuse the run_scope returned by run_task for later get_run_status, get_run_inputs, get_run_outputs, and abort_run calls.
- Use get_run_status to poll progress after launching with wait=false.
- Use get_run_outputs to fetch outputs after completion.
"""


async def initialize_server() -> None:
    await ensure_flyte_initialized()


mcp = FastMCP(
    name="Dynamic Flyte MCP",
    instructions=instructions,
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
    stateless_http=True,
    json_response=True,
)

for tool in STATIC_TOOLS:
    mcp.add_tool(tool)


async def health(request: Request) -> Response:
    return Response(content="OK", media_type="text/plain", status_code=200)


@asynccontextmanager
async def lifespan(_: Starlette) -> AsyncIterator[None]:
    await initialize_server()
    async with mcp.session_manager.run():
        yield


app = Starlette(
    routes=[
        Mount("/sdk", app=mcp.streamable_http_app()),
        Route("/health", health),
    ],
    lifespan=lifespan,
)
