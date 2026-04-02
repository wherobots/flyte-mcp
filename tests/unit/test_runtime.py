import asyncio
from types import SimpleNamespace

import flyte_mcp.runtime as runtime
from flyte_mcp.settings import MCPSettings


def _make_settings(**kwargs) -> MCPSettings:
    defaults = dict(
        deployment_project="my-project",
        deployment_domain="staging",
        execution_project="my-project",
        execution_domain="staging",
    )
    defaults.update(kwargs)
    return MCPSettings(**defaults)


_CAPTURED_KWARGS: list[dict] = []


def _async_capture():
    async def _inner(**kwargs):
        _CAPTURED_KWARGS.append(kwargs)

    return _inner


def _async_fail():
    async def _inner(**kwargs):
        raise AssertionError(f"Unexpected call: {kwargs}")

    return _inner


def test_ensure_flyte_initialized_calls_in_cluster(monkeypatch):
    monkeypatch.setattr(
        runtime,
        "get_settings",
        lambda: _make_settings(
            org="wherobots",
            endpoint="dns:///endpoint",
            deployment_project="deploy-proj",
            deployment_domain="staging",
        ),
    )
    monkeypatch.setattr(
        runtime.flyte, "init_in_cluster", SimpleNamespace(aio=_async_capture())
    )

    asyncio.run(runtime.ensure_flyte_initialized())

    assert _CAPTURED_KWARGS.pop() == {
        "org": "wherobots",
        "project": "deploy-proj",
        "domain": "staging",
        "endpoint": "dns:///endpoint",
    }
