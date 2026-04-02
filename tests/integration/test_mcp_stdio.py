import asyncio
import sys
from pathlib import Path
from typing import Any

import pytest
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

REPO_ROOT = Path(__file__).resolve().parents[2]
REPO_FLYTE_CONFIG = REPO_ROOT / ".flyte" / "config.yaml"
UNION_CONFIG = Path.home() / ".union" / "config.yaml"

pytestmark = pytest.mark.integration


def _require_flyte_config() -> None:
    if not REPO_FLYTE_CONFIG.exists() and not UNION_CONFIG.exists():
        pytest.skip(f"Missing Flyte config: {REPO_FLYTE_CONFIG} or {UNION_CONFIG}")


async def _with_session(callback):
    server = StdioServerParameters(
        command=sys.executable,
        args=["examples/mcp_server.py"],
        cwd=str(REPO_ROOT),
    )
    async with stdio_client(server) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            return await callback(session)


def test_mcp_stdio_lists_registered_stable_tools() -> None:
    _require_flyte_config()

    async def _run(session: ClientSession) -> dict[str, Any]:
        tools = await session.list_tools()
        return {
            tool.name: {
                "description": tool.description,
                "input_schema": tool.inputSchema,
            }
            for tool in tools.tools
        }

    payload = asyncio.run(_with_session(_run))

    assert "list_discovery_projects" in payload
    assert "discover_tasks" in payload
    assert "describe_task" in payload
    assert "run_task" in payload
    assert "get_run_status" in payload
    assert payload["run_task"]["input_schema"]["properties"]["wait"]["default"] is False
