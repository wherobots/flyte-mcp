import asyncio

from flyte_mcp.server import initialize_server, mcp

if __name__ == "__main__":
    asyncio.run(initialize_server())
    mcp.run(transport="stdio")
