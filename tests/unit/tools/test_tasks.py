import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from flyte.io import DataFrame, File
from flyte.remote import TaskDetails
from flyteidl2.task import task_definition_pb2
from google.protobuf.json_format import ParseDict

import flyte_mcp.tools.tasks as tasks_tools
from flyte_mcp.models import DiscoveredTask, StructuredDatasetInput, TaskIdentifier
from flyte_mcp.settings import DiscoveryProject
from flyte_mcp.tools.tasks import (
    _coerce_arguments,
    describe_task,
    discover_tasks,
    list_discovery_projects,
    list_project_versions,
    list_task_versions,
    run_task,
)


def _task_details(task_source: dict[str, Any]) -> TaskDetails:
    return TaskDetails(ParseDict(task_source, task_definition_pb2.TaskDetails()))


def _details(
    name: str,
    *,
    description: str = "A documented task.",
    project: str = "my-project",
    domain: str = "development",
    version: str = "v1",
) -> dict[str, Any]:
    return {
        "taskId": {
            "project": project,
            "domain": domain,
            "name": name,
            "version": version,
        },
        "metadata": {"environmentName": name.split(".", 1)[0]},
        "spec": {
            "taskTemplate": {
                "interface": {
                    "inputs": {
                        "variables": [
                            {
                                "key": "statement",
                                "value": {
                                    "type": {"simple": "STRING"},
                                    "description": "Expression to run.",
                                },
                            }
                        ]
                    },
                    "outputs": {"variables": []},
                }
            },
            "defaultInputs": [],
            "documentation": {"shortDescription": description},
        },
    }


def _all_types_details() -> dict[str, Any]:
    return {
        "taskId": {
            "project": "my-project",
            "domain": "development",
            "name": "root_env.all_types_healthcheck",
            "version": "v1",
        },
        "metadata": {"environmentName": "root_env"},
        "spec": {
            "taskTemplate": {
                "interface": {
                    "inputs": {
                        "variables": [
                            {
                                "key": "e",
                                "value": {
                                    "type": {"structuredDatasetType": {}},
                                    "description": "Structured dataset input.",
                                },
                            },
                            {
                                "key": "g",
                                "value": {
                                    "type": {
                                        "metadata": {
                                            "python_class_name": "<class 'numpy.ndarray'>"
                                        },
                                        "unionType": {
                                            "variants": [
                                                {
                                                    "blob": {"format": "PythonPickle"},
                                                    "structure": {"tag": "FlytePickle"},
                                                },
                                                {
                                                    "simple": "BINARY",
                                                    "structure": {"tag": "FlytePickle"},
                                                },
                                            ]
                                        },
                                    },
                                    "description": "Array input.",
                                },
                            },
                            {
                                "key": "h",
                                "value": {
                                    "type": {"blob": {}},
                                    "description": "Blob input.",
                                },
                            },
                        ]
                    },
                    "outputs": {"variables": []},
                }
            },
            "defaultInputs": [],
            "documentation": {"shortDescription": "All types task."},
        },
    }


def _build_task(task_source: dict[str, Any]) -> DiscoveredTask:
    task = asyncio.run(
        tasks_tools.build_discovered_task(
            _task_details(task_source),
            require_documentation=False,
        )
    )
    assert isinstance(task, DiscoveredTask)
    return task


def test_list_project_versions_uses_requested_projects(monkeypatch) -> None:
    async def fake_list_project_versions(
        *, project: str, domain: str | None, limit: int | None
    ):
        return [f"{project}-v2", f"{project}-v1"]

    monkeypatch.setattr(
        tasks_tools,
        "discover_project_versions",
        fake_list_project_versions,
    )

    payload = asyncio.run(
        list_project_versions(
            projects=["my-project", "my-other-project"],
            domain="development",
        )
    )

    assert payload["domain"] == "development"
    assert payload["projects"][0]["versions"] == [
        "my-project-v2",
        "my-project-v1",
    ]


def test_list_discovery_projects_returns_configured_scope(monkeypatch) -> None:
    monkeypatch.setattr(
        tasks_tools,
        "get_settings",
        lambda: SimpleNamespace(
            discovery_domain="development",
            discovery_projects=(
                DiscoveryProject(project="project-a", version="latest"),
                DiscoveryProject(project="project-b", version="v2"),
            ),
        ),
    )

    payload = asyncio.run(list_discovery_projects())

    assert payload == {
        "domain": "development",
        "count": 2,
        "projects": [
            {"project": "project-a", "version": "latest"},
            {"project": "project-b", "version": "v2"},
        ],
    }


def test_list_task_versions_returns_versions(monkeypatch) -> None:
    async def fake_list_task_versions(
        *, project: str, domain: str | None, task_name: str, limit: int | None
    ):
        assert project == "my-project"
        assert task_name == "root_env.healthcheck"
        return ["v3", "v2", "v1"]

    monkeypatch.setattr(
        tasks_tools,
        "discover_task_versions",
        fake_list_task_versions,
    )

    payload = asyncio.run(
        list_task_versions(
            project="my-project",
            task_name="root_env.healthcheck",
            domain="development",
        )
    )

    assert payload["versions"] == ["v3", "v2", "v1"]


def test_discover_tasks_returns_discovered_and_skipped(monkeypatch) -> None:
    documented = _build_task(_details("root_env.good_task"))

    async def fake_discover_remote_tasks(*, projects, domain, limit):
        return [documented], [
            SimpleNamespace(
                task_name="root_env.hidden_task",
                project="my-project",
                domain=domain,
                environment="root_env",
                reason="missing documentation.shortDescription",
            )
        ]

    monkeypatch.setattr(
        tasks_tools, "discover_remote_tasks", fake_discover_remote_tasks
    )

    payload = asyncio.run(
        discover_tasks(
            projects=[DiscoveryProject(project="my-project", version="v2")],
            domain="development",
        )
    )

    assert payload["count"] == 1
    assert payload["skipped_count"] == 1
    assert payload["tasks"][0]["task_name"] == "root_env.good_task"
    assert payload["tasks"][0]["task_identifier"] == {
        "project": "my-project",
        "domain": "development",
        "version": "v1",
        "task_name": "root_env.good_task",
    }
    assert payload["task_groups"] == [
        {
            "project": "my-project",
            "domain": "development",
            "version": "v1",
            "count": 1,
            "tasks": payload["tasks"],
        }
    ]
    assert payload["skipped"][0]["reason"] == "missing documentation.shortDescription"


def test_describe_task_renders_task_metadata(monkeypatch) -> None:
    monkeypatch.setattr(
        tasks_tools,
        "fetch_task_details",
        lambda **_: asyncio.sleep(
            0, result=_task_details(_details("root_env.good_task"))
        ),
    )

    payload = asyncio.run(
        describe_task(
            project="my-project",
            task_name="root_env.good_task",
            version="latest",
            domain="development",
        )
    )

    assert payload["task_identifier"] == {
        "project": "my-project",
        "domain": "development",
        "version": "v1",
        "task_name": "root_env.good_task",
    }
    assert payload["inputs"][0]["description"] == "Expression to run."


def test_describe_task_accepts_task_identifier(monkeypatch) -> None:
    monkeypatch.setattr(
        tasks_tools,
        "fetch_task_details",
        lambda **kwargs: asyncio.sleep(
            0,
            result=_task_details(
                _details(
                    kwargs["task_name"],
                    project=kwargs["project"],
                    domain=kwargs["domain"],
                    version=kwargs["version"],
                )
            ),
        ),
    )

    payload = asyncio.run(
        describe_task(
            task_identifier=TaskIdentifier(
                project="my-project",
                domain="development",
                version="v2",
                task_name="root_env.good_task",
            ),
        )
    )

    assert payload["task_identifier"]["project"] == "my-project"
    assert payload["task_identifier"]["domain"] == "development"
    assert payload["task_identifier"]["version"] == "v2"


def test_describe_task_defaults_to_latest_without_task_identifier(monkeypatch) -> None:
    monkeypatch.setattr(
        tasks_tools,
        "fetch_task_details",
        lambda **kwargs: asyncio.sleep(
            0,
            result=_task_details(
                _details(
                    kwargs["task_name"],
                    project=kwargs["project"],
                    domain=kwargs["domain"],
                    version=kwargs["version"],
                )
            ),
        ),
    )

    payload = asyncio.run(
        describe_task(
            project="my-project",
            task_name="root_env.good_task",
            domain="staging",
        )
    )

    assert payload["task_identifier"]["project"] == "my-project"
    assert payload["task_identifier"]["domain"] == "staging"
    assert payload["task_identifier"]["version"] == "latest"


def test_coerce_arguments_wraps_dataframe_file_and_ndarray_inputs() -> None:
    task = _build_task(_all_types_details())

    coerced = _coerce_arguments(
        task,
        {
            "e": "/tmp/test.parquet",
            "g": [1, 2, 3],
            "h": "/tmp/file.txt",
        },
    )

    assert isinstance(coerced["e"], DataFrame)
    assert coerced["e"].uri == "/tmp/test.parquet"
    assert isinstance(coerced["h"], File)
    assert coerced["h"].path == "/tmp/file.txt"
    if hasattr(coerced["g"], "tolist"):
        assert coerced["g"].tolist() == [1, 2, 3]
    else:
        assert coerced["g"] == [1, 2, 3]


def test_coerce_arguments_accepts_reference_models() -> None:
    task = _build_task(_all_types_details())

    coerced = _coerce_arguments(
        task,
        {
            "e": StructuredDatasetInput(uri="s3://bucket/frame", format="parquet"),
            "h": File(path="s3://bucket/file.txt", name="file.txt"),
        },
    )

    assert isinstance(coerced["e"], DataFrame)
    assert coerced["e"].uri == "s3://bucket/frame"
    assert coerced["e"].format == "parquet"
    assert isinstance(coerced["h"], File)
    assert coerced["h"].path == "s3://bucket/file.txt"
    assert coerced["h"].name == "file.txt"


def test_run_task_executes_and_returns_outputs(monkeypatch) -> None:
    task = _build_task(_all_types_details())

    async def fake_fetch_task_details(**_: Any) -> TaskDetails:
        return _task_details(_all_types_details())

    class FakeRun:
        def __init__(self) -> None:
            self.name = "run-123"
            self.url = "https://example.invalid/run-123"
            self.phase = "RUNNING"
            self.wait = SimpleNamespace(aio=self._wait)
            self.outputs = SimpleNamespace(aio=self._outputs)

        def done(self) -> bool:
            return self.phase == "SUCCEEDED"

        async def _wait(self) -> None:
            self.phase = "SUCCEEDED"

        async def _outputs(self) -> dict[str, Any]:
            return {"o0": 1}

    captured: dict[str, Any] = {}

    async def fake_run_remote_task(
        task_obj, *, execution_project, execution_domain, **inputs
    ):
        captured["task"] = task_obj
        captured["execution_project"] = execution_project
        captured["execution_domain"] = execution_domain
        captured["inputs"] = inputs
        return FakeRun()

    monkeypatch.setattr(tasks_tools, "fetch_task_details", fake_fetch_task_details)
    monkeypatch.setattr(tasks_tools, "run_remote_task", fake_run_remote_task)
    monkeypatch.setattr(
        tasks_tools,
        "get_execution_project",
        lambda project, task: project or task.project,
    )
    monkeypatch.setattr(
        tasks_tools,
        "get_execution_domain",
        lambda domain, task: domain or task.domain,
    )
    monkeypatch.setattr(tasks_tools.Task, "get", lambda **kwargs: {"lazy_task": kwargs})

    payload = asyncio.run(
        run_task(
            inputs={
                "e": "/tmp/test.parquet",
                "g": [1, 2, 3],
                "h": "/tmp/file.txt",
            },
            task_identifier=TaskIdentifier(
                project="my-project",
                domain="development",
                version="v1",
                task_name="root_env.all_types_healthcheck",
            ),
            wait=True,
        )
    )

    assert payload["task"]["task_name"] == task.task_name
    assert payload["task"]["version"] == task.version
    assert payload["task"]["task_identifier"] == {
        "project": "my-project",
        "domain": "development",
        "version": "v1",
        "task_name": "root_env.all_types_healthcheck",
    }
    assert payload["phase"] == "succeeded"
    assert payload["outputs"] == {"o0": 1}
    assert payload["run_scope"] == {
        "project": "my-project",
        "domain": "development",
    }
    assert captured["execution_project"] == "my-project"
    assert captured["execution_domain"] == "development"
    assert isinstance(captured["inputs"]["e"], DataFrame)
    assert isinstance(captured["inputs"]["h"], File)
    assert captured["task"]["lazy_task"]["version"] == "v1"


def test_run_task_uses_explicit_execution_overrides(monkeypatch) -> None:
    async def fake_fetch_task_details(**_: Any) -> TaskDetails:
        return _task_details(_all_types_details())

    class FakeRun:
        name = "run-123"
        url = "https://example.invalid/run-123"
        phase = "RUNNING"

        def done(self) -> bool:
            return False

    captured: dict[str, Any] = {}

    async def fake_run_remote_task(
        task_obj, *, execution_project, execution_domain, **inputs
    ):
        captured["task"] = task_obj
        captured["execution_project"] = execution_project
        captured["execution_domain"] = execution_domain
        captured["inputs"] = inputs
        return FakeRun()

    monkeypatch.setattr(tasks_tools, "fetch_task_details", fake_fetch_task_details)
    monkeypatch.setattr(tasks_tools, "run_remote_task", fake_run_remote_task)
    monkeypatch.setattr(
        tasks_tools,
        "get_execution_project",
        lambda project, task: f"resolved:{project}",
    )
    monkeypatch.setattr(
        tasks_tools,
        "get_execution_domain",
        lambda domain, task: f"resolved:{domain}",
    )
    monkeypatch.setattr(tasks_tools.Task, "get", lambda **kwargs: {"lazy_task": kwargs})

    payload = asyncio.run(
        run_task(
            inputs={},
            task_identifier=TaskIdentifier(
                project="my-project",
                domain="development",
                version="v1",
                task_name="root_env.all_types_healthcheck",
            ),
            execution_project="custom-project",
            execution_domain="staging",
        )
    )

    assert captured["execution_project"] == "resolved:custom-project"
    assert captured["execution_domain"] == "resolved:staging"
    assert payload["run_scope"] == {
        "project": "resolved:custom-project",
        "domain": "resolved:staging",
    }


def test_run_task_requires_version_without_task_identifier() -> None:
    with pytest.raises(ValueError, match="version is required"):
        asyncio.run(
            run_task(
                project="my-project",
                task_name="root_env.good_task",
                inputs={},
            )
        )


def test_discover_tasks_rejects_conflicting_projects() -> None:
    with pytest.raises(ValueError, match="conflicting versions"):
        asyncio.run(
            discover_tasks(
                projects=[
                    DiscoveryProject(project="my-project", version="v1"),
                    DiscoveryProject(project="my-project", version="v2"),
                ]
            )
        )
