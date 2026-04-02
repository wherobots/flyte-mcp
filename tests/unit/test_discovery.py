import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any, get_args

import flyte.errors
from flyte.remote import TaskDetails
from flyteidl2.task import task_definition_pb2
from google.protobuf.json_format import ParseDict
from pydantic import TypeAdapter

from flyte_mcp.discovery import (
    build_discovered_task,
    discover_tasks,
    mcp_input_annotation,
    render_task,
)
from flyte_mcp.models import DiscoveredTask, SkippedTask
from flyte_mcp.settings import DiscoveryProject


def _task_details(task_source: dict[str, Any]) -> TaskDetails:
    return TaskDetails(ParseDict(task_source, task_definition_pb2.TaskDetails()))


def _build_task(task_source: dict[str, Any]) -> DiscoveredTask | SkippedTask:
    return asyncio.run(build_discovered_task(_task_details(task_source)))


def _details(
    name: str,
    *,
    description: str | None = "A documented task.",
    project: str = "my-project",
    domain: str = "development",
    version: str = "v1",
) -> dict[str, Any]:
    payload = {
        "taskId": {
            "project": project,
            "domain": domain,
            "name": name,
            "version": version,
        },
        "metadata": {},
        "spec": {
            "environment": {
                "name": name.split(".", 1)[0] if "." in name else None,
            },
            "taskTemplate": {
                "interface": {
                    "inputs": {
                        "variables": [
                            {
                                "key": "statement",
                                "value": {
                                    "type": {"simple": "STRING"},
                                    "description": "Statement.",
                                },
                            }
                        ]
                    },
                    "outputs": {"variables": []},
                }
            },
            "defaultInputs": [],
            "documentation": {},
        },
    }
    if description is not None:
        payload["spec"]["documentation"]["shortDescription"] = description
    return payload


def test_build_discovered_task_keeps_documented_tasks():
    task = _build_task(_details("root_env.mosaic_band_math_workflow"))

    assert isinstance(task, DiscoveredTask)
    assert task.tool_name == "root_env.mosaic_band_math_workflow"
    assert task.environment == "root_env"


def test_build_discovered_task_appends_long_description():
    task = _build_task(
        _details("root_env.mosaic_band_math_workflow", description="Short summary.")
        | {
            "spec": _details(
                "root_env.mosaic_band_math_workflow", description="Short summary."
            )["spec"]
            | {
                "documentation": {
                    "shortDescription": "Short summary.",
                    "longDescription": "Longer contextual details.",
                }
            }
        }
    )

    assert task.description == "Short summary.\n\nLonger contextual details."


def test_build_discovered_task_exposes_reference_object_schema_for_blobs_and_datasets():
    task = _build_task(
        {
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
                "documentation": {"shortDescription": "Test."},
            },
        }
    )

    dataset_schema = TypeAdapter(mcp_input_annotation(task, "e")).json_schema()
    file_schema = TypeAdapter(mcp_input_annotation(task, "h")).json_schema()

    assert dataset_schema["anyOf"][0] == {"type": "string"}
    assert dataset_schema["anyOf"][1]["$ref"].endswith("StructuredDatasetInput")
    assert file_schema["anyOf"][0] == {"type": "string"}
    assert file_schema["anyOf"][1]["$ref"].endswith("File")


def test_build_discovered_task_exposes_nullable_union_schema():
    task = _build_task(
        {
            "taskId": {
                "project": "my-project",
                "domain": "development",
                "name": "root_env.nullable_healthcheck",
                "version": "v1",
            },
            "metadata": {"environmentName": "root_env"},
            "spec": {
                "taskTemplate": {
                    "interface": {
                        "inputs": {
                            "variables": [
                                {
                                    "key": "id",
                                    "value": {
                                        "type": {
                                            "unionType": {
                                                "variants": [
                                                    {"simple": "STRING"},
                                                    {"simple": "NONE"},
                                                ]
                                            }
                                        },
                                        "description": "Nullable string input.",
                                    },
                                },
                                {
                                    "key": "nodata",
                                    "value": {
                                        "type": {
                                            "unionType": {
                                                "variants": [
                                                    {"simple": "STRING"},
                                                    {"simple": "FLOAT"},
                                                    {"simple": "NONE"},
                                                ]
                                            }
                                        },
                                        "description": "String or float or null.",
                                    },
                                },
                            ]
                        },
                        "outputs": {"variables": []},
                    }
                },
                "defaultInputs": [],
                "documentation": {"shortDescription": "Test."},
            },
        }
    )

    id_schema = TypeAdapter(mcp_input_annotation(task, "id")).json_schema()
    nodata_schema = TypeAdapter(mcp_input_annotation(task, "nodata")).json_schema()

    assert id_schema["anyOf"] == [{"type": "string"}, {"type": "null"}]
    assert nodata_schema["anyOf"] == [
        {"type": "string"},
        {"type": "number"},
        {"type": "null"},
    ]


def test_build_discovered_task_uses_typeengine_for_datetime_and_duration():
    task = _build_task(
        {
            "taskId": {
                "project": "my-project",
                "domain": "development",
                "name": "root_env.temporal_healthcheck",
                "version": "v1",
            },
            "metadata": {"environmentName": "root_env"},
            "spec": {
                "taskTemplate": {
                    "interface": {
                        "inputs": {
                            "variables": [
                                {
                                    "key": "ts",
                                    "value": {
                                        "type": {"simple": "DATETIME"},
                                        "description": "Timestamp input.",
                                    },
                                },
                                {
                                    "key": "window",
                                    "value": {
                                        "type": {"simple": "DURATION"},
                                        "description": "Duration input.",
                                    },
                                },
                            ]
                        },
                        "outputs": {"variables": []},
                    }
                },
                "defaultInputs": [],
                "documentation": {"shortDescription": "Test."},
            },
        }
    )

    assert get_args(mcp_input_annotation(task, "ts"))[0] is datetime
    assert get_args(mcp_input_annotation(task, "window"))[0] is timedelta


def test_build_discovered_task_converts_pickle_backed_ndarray_to_array_schema():
    task = _build_task(
        {
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
                                    "key": "g",
                                    "value": {
                                        "type": {
                                            "metadata": {
                                                "python_class_name": "<class 'numpy.ndarray'>"
                                            },
                                            "unionType": {
                                                "variants": [
                                                    {
                                                        "blob": {
                                                            "format": "PythonPickle"
                                                        },
                                                        "structure": {
                                                            "tag": "FlytePickle"
                                                        },
                                                    },
                                                    {
                                                        "simple": "BINARY",
                                                        "structure": {
                                                            "tag": "FlytePickle"
                                                        },
                                                    },
                                                ]
                                            },
                                        },
                                        "description": "Array input.",
                                    },
                                }
                            ]
                        },
                        "outputs": {"variables": []},
                    }
                },
                "defaultInputs": [],
                "documentation": {"shortDescription": "Test."},
            },
        }
    )

    array_schema = TypeAdapter(mcp_input_annotation(task, "g")).json_schema()

    assert array_schema["type"] == "array"


def test_render_task_exposes_mcp_and_flyte_schemas():
    task = _build_task(
        {
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
                                    "key": "g",
                                    "value": {
                                        "type": {
                                            "metadata": {
                                                "python_class_name": "<class 'numpy.ndarray'>"
                                            },
                                            "unionType": {
                                                "variants": [
                                                    {
                                                        "blob": {
                                                            "format": "PythonPickle"
                                                        },
                                                        "structure": {
                                                            "tag": "FlytePickle"
                                                        },
                                                    },
                                                    {
                                                        "simple": "BINARY",
                                                        "structure": {
                                                            "tag": "FlytePickle"
                                                        },
                                                    },
                                                ]
                                            },
                                        },
                                        "description": "Array input.",
                                    },
                                }
                            ]
                        },
                        "outputs": {"variables": []},
                    }
                },
                "defaultInputs": [],
                "documentation": {"shortDescription": "Test."},
            },
        }
    )

    rendered = render_task(task)

    assert rendered.inputs[0].input_schema["type"] == "array"
    assert rendered.inputs[0].flyte_input_schema["format"] == "union"


def test_render_task_serializes_enum_defaults():
    task = _build_task(
        {
            "taskId": {
                "project": "my-project",
                "domain": "development",
                "name": "root_env.enum_healthcheck",
                "version": "v1",
            },
            "metadata": {"environmentName": "root_env"},
            "spec": {
                "taskTemplate": {
                    "interface": {
                        "inputs": {
                            "variables": [
                                {
                                    "key": "runtime",
                                    "value": {
                                        "type": {
                                            "enumType": {
                                                "values": ["SMALL", "MEDIUM"],
                                            }
                                        },
                                        "description": "Runtime size.",
                                    },
                                }
                            ]
                        },
                        "outputs": {"variables": []},
                    }
                },
                "defaultInputs": [
                    {
                        "name": "runtime",
                        "parameter": {
                            "default": {
                                "scalar": {
                                    "primitive": {
                                        "stringValue": "SMALL",
                                    }
                                }
                            }
                        },
                    }
                ],
                "documentation": {"shortDescription": "Test."},
            },
        }
    )

    rendered = render_task(task)

    assert rendered.defaults == {"runtime": "SMALL"}


def test_build_discovered_task_skips_undocumented_tasks():
    skipped = _build_task(_details("root_env.hidden_task", description=None))

    assert isinstance(skipped, SkippedTask)
    assert skipped.reason == "missing documentation.shortDescription"


def test_build_discovered_task_can_allow_undocumented_tasks():
    task = asyncio.run(
        build_discovered_task(
            _task_details(_details("root_env.hidden_task", description=None)),
            require_documentation=False,
        )
    )

    assert isinstance(task, DiscoveredTask)
    assert task.description == "root_env.hidden_task"


def test_discover_tasks_collects_documented_and_skipped(monkeypatch):
    async def fake_list_task_names(project: str, domain: str, limit: int):
        return ["root_env.good_task", "root_env.undocumented_task"]

    async def fake_fetch_task_details(
        *, project: str, domain: str, task_name: str, version: str
    ):
        assert version == "v2"
        if task_name == "root_env.undocumented_task":
            return _task_details(_details(task_name, description=None))
        return _task_details(_details(task_name))

    monkeypatch.setattr("flyte_mcp.discovery._list_task_names", fake_list_task_names)
    monkeypatch.setattr(
        "flyte_mcp.discovery.fetch_task_details", fake_fetch_task_details
    )

    discovered, skipped = asyncio.run(
        discover_tasks(
            projects=(DiscoveryProject(project="my-project", version="v2"),),
            domain="development",
            limit=10,
            concurrency=2,
        )
    )

    assert [task.tool_name for task in discovered] == ["root_env.good_task"]
    assert [task.task_name for task in skipped] == ["root_env.undocumented_task"]


def test_discover_tasks_skips_missing_exact_version(monkeypatch):
    async def fake_list_task_names(project: str, domain: str, limit: int):
        return ["root_env.good_task", "root_env.missing_task"]

    async def fake_fetch_task_details(
        *, project: str, domain: str, task_name: str, version: str
    ):
        assert version == "v2"
        if task_name == "root_env.missing_task":
            raise flyte.errors.RemoteTaskNotFoundError(
                "Task root_env.missing_task, version v2 not found in my-project development."
            )
        return _task_details(_details(task_name, version="v2"))

    monkeypatch.setattr("flyte_mcp.discovery._list_task_names", fake_list_task_names)
    monkeypatch.setattr(
        "flyte_mcp.discovery.fetch_task_details", fake_fetch_task_details
    )

    discovered, skipped = asyncio.run(
        discover_tasks(
            projects=(DiscoveryProject(project="my-project", version="v2"),),
            domain="development",
            limit=10,
            concurrency=2,
        )
    )

    assert [task.tool_name for task in discovered] == ["root_env.good_task"]
    assert len(skipped) == 1
    assert skipped[0].task_name == "root_env.missing_task"
    assert skipped[0].reason == "task version v2 not found"


def test_discover_tasks_still_fetches_exact_version_when_listall_shows_newer_one(
    monkeypatch,
):
    async def fake_listall(**kwargs: Any):
        assert kwargs == {
            "project": "my-project",
            "domain": "development",
            "limit": 10,
        }
        yield SimpleNamespace(name="root_env.good_task", version="v3")

    async def fake_fetch_task_details(
        *, project: str, domain: str, task_name: str, version: str
    ):
        assert project == "my-project"
        assert domain == "development"
        assert task_name == "root_env.good_task"
        assert version == "v2"
        return _task_details(_details(task_name, version="v2"))

    monkeypatch.setattr(
        "flyte_mcp.discovery.Task.listall",
        SimpleNamespace(aio=fake_listall),
    )
    monkeypatch.setattr(
        "flyte_mcp.discovery.fetch_task_details", fake_fetch_task_details
    )

    discovered, skipped = asyncio.run(
        discover_tasks(
            projects=(DiscoveryProject(project="my-project", version="v2"),),
            domain="development",
            limit=10,
            concurrency=2,
        )
    )

    assert [task.tool_name for task in discovered] == ["root_env.good_task"]
    assert discovered[0].version == "v2"
    assert skipped == []
