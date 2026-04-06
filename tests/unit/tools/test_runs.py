import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import flyte_mcp.tools.runs as run_tools
from flyte.remote import TimeFilter
from flyte_mcp.tools.runs import (
    abort_run,
    get_run_inputs,
    get_run_outputs,
    get_run_status,
    list_runs,
)


def test_list_runs_uses_requested_scope_task_filter_and_sort(monkeypatch):
    async def fake_details_aio():
        return SimpleNamespace(
            action_details=SimpleNamespace(
                task_name="root_env.task",
                error_info=None,
            )
        )

    fake_run = SimpleNamespace(
        name="run-9",
        phase="succeeded",
        url="https://console/run-9",
        done=lambda: True,
        details=SimpleNamespace(aio=fake_details_aio),
    )

    async def fake_listall(**kwargs):
        assert kwargs["project"] == "other-project"
        assert kwargs["domain"] == "development"
        assert kwargs["in_phase"] is None
        assert kwargs["task_name"] == "root_env.task"
        assert kwargs["sort_by"] == ("created_at", "desc")
        assert kwargs["limit"] == 5
        yield fake_run

    monkeypatch.setattr(run_tools.Run, "listall", SimpleNamespace(aio=fake_listall))

    payload = asyncio.run(
        list_runs(
            limit=5,
            task_name="root_env.task",
            project="other-project",
            domain="development",
        )
    )

    assert payload["project"] == "other-project"
    assert payload["domain"] == "development"
    assert payload["task_name"] == "root_env.task"
    assert payload["count"] == 1
    assert payload["in_phase"] is None
    assert payload["runs"][0]["run_name"] == "run-9"


def test_list_runs_uses_execution_scope_and_phase_filter(monkeypatch):
    async def fake_details_aio():
        return SimpleNamespace(
            action_details=SimpleNamespace(
                task_name="root_env.task",
                error_info=None,
            )
        )

    fake_run = SimpleNamespace(
        name="run-0",
        phase="running",
        url="https://console/run-0",
        done=lambda: False,
        details=SimpleNamespace(aio=fake_details_aio),
    )

    async def fake_listall(**kwargs):
        assert kwargs["project"] == "isolated-project"
        assert kwargs["domain"] == "isolated-domain"
        assert kwargs["in_phase"] == ("queued", "running")
        yield fake_run

    monkeypatch.setattr(
        run_tools,
        "get_execution_project",
        lambda project=None, task=None: "isolated-project",
    )
    monkeypatch.setattr(
        run_tools,
        "get_execution_domain",
        lambda domain=None, task=None: "isolated-domain",
    )
    monkeypatch.setattr(run_tools.Run, "listall", SimpleNamespace(aio=fake_listall))

    payload = asyncio.run(list_runs(limit=25, in_phase=["queued", "running"]))

    assert payload["project"] == "isolated-project"
    assert payload["domain"] == "isolated-domain"
    assert payload["in_phase"] == ["queued", "running"]
    assert payload["count"] == 1
    assert payload["runs"][0]["run_name"] == "run-0"


def test_list_runs_forwards_all_listall_filters(monkeypatch):
    created_at = TimeFilter(
        after=datetime(2025, 1, 1, tzinfo=timezone.utc),
        before=datetime(2025, 2, 1, tzinfo=timezone.utc),
    )
    updated_at = TimeFilter(
        after=datetime(2025, 3, 1, tzinfo=timezone.utc),
        before=datetime(2025, 4, 1, tzinfo=timezone.utc),
    )

    async def fake_details_aio():
        return SimpleNamespace(
            action_details=SimpleNamespace(
                task_name="root_env.task",
                error_info=None,
            )
        )

    fake_run = SimpleNamespace(
        name="run-10",
        phase="succeeded",
        url="https://console/run-10",
        done=lambda: True,
        details=SimpleNamespace(aio=fake_details_aio),
    )

    async def fake_listall(**kwargs):
        assert kwargs["task_name"] == "root_env.task"
        assert kwargs["task_version"] == "v123"
        assert kwargs["created_by_subject"] == "user-123"
        assert kwargs["sort_by"] == ("updated_at", "asc")
        assert kwargs["created_at"] == created_at
        assert kwargs["updated_at"] == updated_at
        yield fake_run

    monkeypatch.setattr(run_tools.Run, "listall", SimpleNamespace(aio=fake_listall))

    payload = asyncio.run(
        list_runs(
            task_name="root_env.task",
            task_version="v123",
            created_by_subject="user-123",
            sort_by=["updated_at", "asc"],
            project="other-project",
            domain="development",
            created_at=created_at,
            updated_at=updated_at,
        )
    )

    assert payload["task_version"] == "v123"
    assert payload["created_by_subject"] == "user-123"
    assert payload["sort_by"] == ["updated_at", "asc"]
    assert payload["created_at"] == {
        "after": "2025-01-01T00:00:00+00:00",
        "before": "2025-02-01T00:00:00+00:00",
    }
    assert payload["updated_at"] == {
        "after": "2025-03-01T00:00:00+00:00",
        "before": "2025-04-01T00:00:00+00:00",
    }


def test_list_runs_normalizes_uppercase_sort_direction(monkeypatch):
    async def fake_details_aio():
        return SimpleNamespace(
            action_details=SimpleNamespace(
                task_name="root_env.task",
                error_info=None,
            )
        )

    fake_run = SimpleNamespace(
        name="run-11",
        phase="succeeded",
        url="https://console/run-11",
        done=lambda: True,
        details=SimpleNamespace(aio=fake_details_aio),
    )

    async def fake_listall(**kwargs):
        assert kwargs["sort_by"] == ("updated_at", "desc")
        yield fake_run

    monkeypatch.setattr(
        run_tools,
        "get_execution_project",
        lambda project=None, task=None: "isolated-project",
    )
    monkeypatch.setattr(
        run_tools,
        "get_execution_domain",
        lambda domain=None, task=None: "isolated-domain",
    )
    monkeypatch.setattr(run_tools.Run, "listall", SimpleNamespace(aio=fake_listall))

    payload = asyncio.run(list_runs(sort_by=["updated_at", "DESC"]))

    assert payload["sort_by"] == ["updated_at", "desc"]


def test_list_runs_rejects_invalid_sort_direction():
    try:
        run_tools._normalize_sort_by(["updated_at", "newest"])
    except ValueError as exc:
        assert str(exc) == "sort_by direction must be 'asc' or 'desc'"
    else:
        raise AssertionError("Expected ValueError")


def test_get_run_status_returns_phase_payload(monkeypatch):
    async def fake_details():
        return SimpleNamespace(
            action_details=SimpleNamespace(
                task_name="root_env.task",
                error_info=None,
            )
        )

    fake_details = SimpleNamespace(aio=fake_details)
    fake_run = SimpleNamespace(
        name="run-1",
        phase="running",
        url="https://console/run-1",
        done=lambda: False,
        details=fake_details,
    )

    monkeypatch.setattr(
        run_tools.Run, "get", SimpleNamespace(aio=_async_return(fake_run))
    )

    payload = asyncio.run(get_run_status("run-1"))

    assert payload["run_name"] == "run-1"
    assert payload["phase"] == "running"
    assert payload["done"] is False


def test_get_run_inputs_returns_named_inputs(monkeypatch):
    fake_inputs = SimpleNamespace(named_outputs={"statement": "a = b"})

    async def fake_inputs_aio():
        return fake_inputs

    fake_run = SimpleNamespace(
        name="run-inputs",
        phase="running",
        url="https://console/run-inputs",
        done=lambda: False,
        inputs=SimpleNamespace(aio=fake_inputs_aio),
    )

    monkeypatch.setattr(
        run_tools.Run, "get", SimpleNamespace(aio=_async_return(fake_run))
    )

    payload = asyncio.run(get_run_inputs("run-inputs"))

    assert payload["run_name"] == "run-inputs"
    assert payload["inputs"] == {"statement": "a = b"}


def test_get_run_outputs_handles_not_ready_run(monkeypatch):
    fake_run = SimpleNamespace(
        name="run-2",
        phase="queued",
        url="https://console/run-2",
        done=lambda: False,
    )

    monkeypatch.setattr(
        run_tools.Run, "get", SimpleNamespace(aio=_async_return(fake_run))
    )

    payload = asyncio.run(get_run_outputs("run-2"))

    assert payload["ready"] is False
    assert payload["outputs"] is None


def test_get_run_outputs_returns_named_outputs(monkeypatch):
    fake_outputs = SimpleNamespace(named_outputs={"o0": "s3://bucket/output"})

    async def fake_outputs_aio():
        return fake_outputs

    fake_run = SimpleNamespace(
        name="run-3",
        phase="succeeded",
        url="https://console/run-3",
        done=lambda: True,
        outputs=SimpleNamespace(aio=fake_outputs_aio),
    )

    monkeypatch.setattr(
        run_tools.Run, "get", SimpleNamespace(aio=_async_return(fake_run))
    )

    payload = asyncio.run(get_run_outputs("run-3"))

    assert payload["ready"] is True
    assert payload["outputs"] == {"o0": "s3://bucket/output"}


def test_abort_run_requests_abort_and_returns_status(monkeypatch):
    aborts: list[str] = []

    async def fake_abort(reason: str):
        aborts.append(reason)

    async def fake_details():
        return SimpleNamespace(
            action_details=SimpleNamespace(
                task_name="root_env.task",
                error_info=None,
            )
        )

    fake_run = SimpleNamespace(
        name="run-4",
        phase="aborted",
        url="https://console/run-4",
        done=lambda: True,
        abort=SimpleNamespace(aio=fake_abort),
        details=SimpleNamespace(aio=fake_details),
    )

    monkeypatch.setattr(
        run_tools.Run, "get", SimpleNamespace(aio=_async_return(fake_run))
    )

    payload = asyncio.run(abort_run("run-4", reason="stop it"))

    assert aborts == ["stop it"]
    assert payload["run_name"] == "run-4"
    assert payload["abort_requested"] is True
    assert payload["abort_reason"] == "stop it"


def _async_return(value):
    async def _inner(**kwargs):
        return value

    return _inner
