from flyte_mcp.tools.runs import RUN_TOOLS
from flyte_mcp.tools.tasks import TASK_TOOLS

STATIC_TOOLS = [*TASK_TOOLS, *RUN_TOOLS]

__all__ = [
    "TASK_TOOLS",
    "RUN_TOOLS",
    "STATIC_TOOLS",
]
