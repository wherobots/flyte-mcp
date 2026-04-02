import asyncio

import flyte_mcp.server as server


def test_initialize_server_calls_flyte_init(monkeypatch) -> None:
    called = {"count": 0}

    async def fake_ensure_flyte_initialized() -> None:
        called["count"] += 1

    monkeypatch.setattr(
        server, "ensure_flyte_initialized", fake_ensure_flyte_initialized
    )

    asyncio.run(server.initialize_server())

    assert called["count"] == 1


def test_static_tools_are_registered() -> None:
    tool_names = [tool.name for tool in server.mcp._tool_manager.list_tools()]

    assert "list_discovery_projects" in tool_names
    assert "discover_tasks" in tool_names
    assert "describe_task" in tool_names
    assert "run_task" in tool_names
    assert "list_project_versions" in tool_names
    assert "list_task_versions" in tool_names
    assert "list_runs" in tool_names
