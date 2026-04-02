from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


async def _inspect(server_script: str, cwd: Path) -> dict[str, Any]:
    server = StdioServerParameters(
        command=sys.executable,
        args=[server_script],
        cwd=str(cwd),
    )
    async with stdio_client(server) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            tools = await session.list_tools()
            return {
                "tool_count": len(tools.tools),
                "tool_names": [tool.name for tool in tools.tools],
            }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--server-script",
        default="examples/mcp_server.py",
        help="Relative path to MCP stdio server script.",
    )
    parser.add_argument(
        "--cwd",
        default=".",
        help="Working directory for launching the MCP server.",
    )
    args = parser.parse_args()

    payload = asyncio.run(
        _inspect(server_script=args.server_script, cwd=Path(args.cwd))
    )
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
