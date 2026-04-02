import asyncio
import os
import sys
import tempfile
from datetime import timedelta
from pathlib import Path
from typing import Any

import flyte
import flyte.remote as remote
import geopandas as gpd
import pandas as pd
import pytest
from flyte.io import DataFrame
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from shapely.geometry import Point

REPO_ROOT = Path(__file__).resolve().parents[2]
REPO_FLYTE_CONFIG = REPO_ROOT / ".flyte" / "config.yaml"
UNION_CONFIG = Path.home() / ".union" / "config.yaml"
TASK_NAME = "root_env.all_types_healthcheck"
DISCOVERY_PROJECTS = "wherobots-rasterflow:pr-230-2c074d"

pytestmark = pytest.mark.integration


def _require_flyte_config() -> None:
    if not REPO_FLYTE_CONFIG.exists() and not UNION_CONFIG.exists():
        pytest.skip(f"Missing Flyte config: {REPO_FLYTE_CONFIG} or {UNION_CONFIG}")


async def _with_session(callback):
    server = StdioServerParameters(
        command=sys.executable,
        args=["examples/mcp_server.py"],
        cwd=str(REPO_ROOT),
        env={
            **os.environ,
            "FLYTE_MCP_DISCOVERY_PROJECTS": DISCOVERY_PROJECTS,
        },
    )
    async with stdio_client(server) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            return await callback(session)


def test_mcp_stdio_discovers_all_types_healthcheck_task() -> None:
    _require_flyte_config()

    async def _run(session: ClientSession) -> dict[str, Any]:
        result = await session.call_tool(
            "discover_tasks",
            {
                "projects": [
                    {
                        "project": "wherobots-rasterflow",
                        "version": "pr-230-2c074d",
                    }
                ]
            },
        )
        return result.structuredContent

    payload = asyncio.run(_with_session(_run))

    task = next(item for item in payload["tasks"] if item["task_name"] == TASK_NAME)
    assert task["description"]
    assert task["version"] == "pr-230-2c074d"


def test_mcp_stdio_calls_all_types_healthcheck_round_trip() -> None:
    _require_flyte_config()

    async def _run(session: ClientSession) -> dict[str, Any]:
        flyte.init_from_config()

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)

            geo_frame = gpd.GeoDataFrame(
                {"id": [1], "name": ["alpha"]},
                geometry=[Point(1.0, 2.0)],
                crs="EPSG:4326",
            )
            geo_path = tmp_path / "data.geojson"
            geo_frame.to_file(geo_path, driver="GeoJSON")
            _, geo_remote_uri = remote.upload_file(geo_path)

            dataframe_uri = DataFrame.from_local_sync(
                pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
            ).model_dump()["uri"]

            note_path = tmp_path / "note.txt"
            note_path.write_text("hello integration\n")

            result = await session.call_tool(
                "run_task",
                {
                    "project": "wherobots-rasterflow",
                    "task_name": TASK_NAME,
                    "version": "pr-230-2c074d",
                    "inputs": {
                        "a": 7,
                        "b": 3.5,
                        "c": "integration",
                        "d": True,
                        "e": {"uri": geo_remote_uri, "format": ""},
                        "f": {"uri": dataframe_uri, "format": "parquet"},
                        "g": [1, 2, 3],
                        "h": {"path": str(note_path), "name": "note.txt"},
                    },
                    "wait": True,
                },
                read_timeout_seconds=timedelta(seconds=180),
            )
            return {
                "is_error": result.isError,
                "structured_content": result.structuredContent,
            }

    payload = asyncio.run(_with_session(_run))

    assert payload["is_error"] is False
    assert payload["structured_content"]["task"]["task_name"] == TASK_NAME
    assert payload["structured_content"]["phase"] == "succeeded"
    assert payload["structured_content"]["done"] is True
    assert payload["structured_content"]["success"] is True
    assert payload["structured_content"]["outputs"]["o0"] == 7
    assert payload["structured_content"]["outputs"]["o1"] == 3.5
    assert payload["structured_content"]["outputs"]["o2"] == "integration"
    assert payload["structured_content"]["outputs"]["o3"] is True
    assert payload["structured_content"]["outputs"]["o4"]["uri"].startswith("s3://")
    assert (
        payload["structured_content"]["outputs"]["o4"]["format"] == "geopandas/parquet"
    )
    assert payload["structured_content"]["outputs"]["o5"]["uri"].startswith("s3://")
    assert payload["structured_content"]["outputs"]["o5"]["format"] == "parquet"
    assert payload["structured_content"]["outputs"]["o6"] == [1, 2, 3]
    assert payload["structured_content"]["outputs"]["o7"]["name"] == "note.txt"


def test_mcp_stdio_fetches_all_types_healthcheck_outputs_by_run_name() -> None:
    _require_flyte_config()

    async def _run(session: ClientSession) -> dict[str, Any]:
        flyte.init_from_config()

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)

            geo_frame = gpd.GeoDataFrame(
                {"id": [1], "name": ["alpha"]},
                geometry=[Point(1.0, 2.0)],
                crs="EPSG:4326",
            )
            geo_path = tmp_path / "data.geojson"
            geo_frame.to_file(geo_path, driver="GeoJSON")
            _, geo_remote_uri = remote.upload_file(geo_path)

            dataframe_uri = DataFrame.from_local_sync(
                pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
            ).model_dump()["uri"]

            note_path = tmp_path / "note.txt"
            note_path.write_text("hello integration\n")

            launch = await session.call_tool(
                "run_task",
                {
                    "project": "wherobots-rasterflow",
                    "task_name": TASK_NAME,
                    "version": "pr-230-2c074d",
                    "inputs": {
                        "a": 7,
                        "b": 3.5,
                        "c": "integration",
                        "d": True,
                        "e": {"uri": geo_remote_uri, "format": ""},
                        "f": {"uri": dataframe_uri, "format": "parquet"},
                        "g": [1, 2, 3],
                        "h": {"path": str(note_path), "name": "note.txt"},
                    },
                    "wait": False,
                },
                read_timeout_seconds=timedelta(seconds=60),
            )
            run_name = launch.structuredContent["run_name"]

            outputs = await session.call_tool(
                "get_run_outputs",
                {
                    "run_name": run_name,
                    "wait": True,
                },
                read_timeout_seconds=timedelta(seconds=180),
            )
            return {
                "launch_error": launch.isError,
                "run_name": run_name,
                "outputs_error": outputs.isError,
                "outputs": outputs.structuredContent,
            }

    payload = asyncio.run(_with_session(_run))

    assert payload["launch_error"] is False
    assert payload["run_name"]
    assert payload["outputs_error"] is False
    assert payload["outputs"]["phase"] == "succeeded"
    assert payload["outputs"]["done"] is True
    assert payload["outputs"]["success"] is True
    assert payload["outputs"]["ready"] is True
    assert payload["outputs"]["outputs"]["o0"] == 7
    assert payload["outputs"]["outputs"]["o1"] == 3.5
    assert payload["outputs"]["outputs"]["o2"] == "integration"
    assert payload["outputs"]["outputs"]["o3"] is True
    assert payload["outputs"]["outputs"]["o4"]["uri"].startswith("s3://")
    assert payload["outputs"]["outputs"]["o4"]["format"] == "geopandas/parquet"
    assert payload["outputs"]["outputs"]["o5"]["uri"].startswith("s3://")
    assert payload["outputs"]["outputs"]["o5"]["format"] == "parquet"
    assert payload["outputs"]["outputs"]["o6"] == [1, 2, 3]
    assert payload["outputs"]["outputs"]["o7"]["name"] == "note.txt"
