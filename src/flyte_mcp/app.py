from pathlib import Path

import flyte
import uvicorn
from flyte.app import AppEnvironment, Domain, Link, Scaling

from flyte_mcp.settings import get_settings

PORT = 8000
APP_NAME = "flyte-mcp"
SCALE_DOWN_SECONDS = 3600

image = (
    flyte.Image.from_debian_base()
    .with_source_file(Path("deploy.yaml"))
    .with_dockerignore(Path(".dockerignore"))
    .with_uv_project(
        pyproject_file=Path("pyproject.toml"),
        uvlock=Path("uv.lock"),
        project_install_mode="install_project",
    )
)

app = AppEnvironment(
    name=APP_NAME,
    port=PORT,
    include=["app.py", "../../deploy.yaml"],
    image=image,
    domain=Domain(subdomain=APP_NAME),
    resources=flyte.Resources(cpu=2, memory="4Gi", disk="8Gi"),
    requires_auth=True,
    scaling=Scaling(replicas=(0, 1), scaledown_after=SCALE_DOWN_SECONDS),
    links=[
        Link(
            path="/sdk/mcp",
            title="Streamable HTTP transport endpoint",
            is_relative=True,
        ),
        Link(path="/health", title="Health check endpoint", is_relative=True),
    ],
)


@app.server
async def server() -> None:
    from flyte_mcp.server import app as mcp_app

    server = uvicorn.Server(uvicorn.Config(mcp_app, port=PORT, log_level="info"))
    await server.serve()


if __name__ == "__main__":
    config = get_settings()
    flyte.init_from_config(image_builder="remote")
    flyte.with_servecontext(
        mode="remote",
        project=config.execution_project,
        domain=config.execution_domain,
    ).serve(app)
