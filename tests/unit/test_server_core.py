import asyncio
import inspect

import flyte_mcp.server as server
from pydantic import create_model

from flyte_mcp.tools.runs import list_runs


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


def test_list_runs_schema_uses_array_items_for_sort_by() -> None:
    fields = {}
    for name, param in inspect.signature(list_runs).parameters.items():
        default = ... if param.default is inspect._empty else param.default
        fields[name] = (param.annotation, default)

    schema = create_model("list_runs_params", **fields).model_json_schema()
    sort_by_schema = schema["properties"]["sort_by"]["anyOf"][0]

    assert sort_by_schema["type"] == "array"
    assert sort_by_schema["items"] == {"type": "string"}
    assert sort_by_schema["minItems"] == 2
    assert sort_by_schema["maxItems"] == 2
    assert "prefixItems" not in sort_by_schema
