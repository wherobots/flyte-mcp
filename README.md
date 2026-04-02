# flyte-mcp

An MCP server that lets AI agents discover, describe, and run [Flyte](https://flyte.org) tasks via the [Model Context Protocol](https://modelcontextprotocol.io).

## How it works

The server exposes a set of static MCP tools for interacting with Flyte:

- **`list_discovery_projects`** — list the configured default discovery project/version scope
- **`discover_tasks`** — list documented tasks across one or more projects/versions
- **`describe_task`** — fetch full input schemas and defaults for a specific task
- **`run_task`** — launch a task execution with validated inputs
- **`list_project_versions`** / **`list_task_versions`** — inspect available versions
- **`list_runs`** / **`get_run_status`** / **`get_run_outputs`** / **`abort_run`** — monitor and manage runs

An agent can call `list_discovery_projects` first to learn the configured default discovery scope, remember that project set for later calls, then use `discover_tasks` to find what's available, `describe_task` to understand inputs, and `run_task` to execute — passing back the `task_identifier` returned by discovery to ensure the exact intended version is run.

## Deploying your own instance

See the `deploy-mcp-app` target in the [Makefile](Makefile) for a full example with image build and push.

## Add to Codex

Once deployed, register it in Codex with your API key:

```bash
export FLYTE_API_KEY=<your-api-key>

codex mcp add <name> \
    --url <your-app-url>/sdk/mcp \
    --bearer-token-env-var FLYTE_API_KEY
```

Codex reads the bearer token from the named environment variable when it connects.

## Add to OpenCode

Once deployed, register it in OpenCode with your API key:

```bash
export FLYTE_API_KEY=<your-api-key>

make add-opencode-mcp
```

This upserts a `flyte-mcp` entry in `~/.config/opencode/opencode.json` that points to the deployed MCP URL and stores the auth header as `Bearer {env:FLYTE_API_KEY}`, so OpenCode resolves the token from your environment at connection time.

## Add to Claude

Once deployed, register it in Claude Code with your API key:

```bash
export FLYTE_API_KEY=<your-api-key>

claude mcp add --transport http <name> \
    <your-app-url>/sdk/mcp \
    --scope user \
    --header 'Authorization: Bearer ${FLYTE_API_KEY}'
```

The `${FLYTE_API_KEY}` reference is stored unexpanded, so Claude Code resolves it from your environment at connection time instead of writing the token to disk.

## Configuration

Server configuration is loaded from [`deploy.yaml`](deploy.yaml) and validated by [`MCPSettings`](src/flyte_mcp/settings.py).

For the supported fields, defaults, and semantics, see the `MCPSettings` docstring in [`src/flyte_mcp/settings.py`](src/flyte_mcp/settings.py). That file is the source of truth for deployment, execution, and discovery configuration, including the `DiscoveryProject` shape used by `discovery_projects`.
