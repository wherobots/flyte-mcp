APP_FILE ?= src/flyte_mcp/app.py
FLYTE_CONFIG ?= .flyte/config.yaml
USER ?= $(shell printf '%s' "$$USER")
SUBDOMAIN = flyte-mcp
ORG ?= wherobots
REGION ?= us-west-2

.PHONY: help setup install-pre-commit pre-commit test test-unit test-integration deploy-mcp-app clear-flyte-keyring create-test-api-key delete-test-api-key add-claude-mcp remove-claude-mcp add-codex-mcp remove-codex-mcp

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-24s %s\n", $$1, $$2}'

setup: ## Install project dependencies with uv
	uv sync

install-pre-commit: ## Install pre-commit hooks into .git/hooks
	uv run pre-commit install

pre-commit: ## Run pre-commit hooks on all files
	uv run pre-commit run -a

test: ## Run all tests
	uv run pytest

test-unit: ## Run unit tests
	uv run pytest tests/unit -q

test-integration: ## Run integration tests
	uv run pytest tests/integration -q

deploy-mcp-app: ## Deploy the Flyte AppEnvironment (image built remotely by the platform)
	uv run python src/flyte_mcp/app.py

create-test-api-key: ## Create Flyte API key named {USER}-mcp-app
	@test -n "$(USER)" || (echo "USER is required. Usage: make $@ USER=<user>" >&2; exit 1)
	uv run --with flyteplugins-union flyte create api-key --name $(USER)-mcp-app

delete-test-api-key: ## Delete Flyte API key named {USER}-mcp-app
	@test -n "$(USER)" || (echo "USER is required. Usage: make $@ USER=<user>" >&2; exit 1)
	uv run --with flyteplugins-union flyte delete api-key $(USER)-mcp-app

add-claude-mcp: ## Register the wherobots-rasterflow MCP server in Claude Code at user scope (requires FLYTE_API_KEY)
	claude mcp add \
		--transport http \
		--scope user \
		--header 'Authorization: Bearer $${FLYTE_API_KEY}' \
		-- \
		flyte-mcp \
		https://$(SUBDOMAIN).apps.$(ORG).$(REGION).unionai.cloud/sdk/mcp

remove-claude-mcp: ## Remove the wherobots-rasterflow MCP server from Claude Code
	claude mcp remove wherobots-flyte-mcp

add-codex-mcp: ## Register the wherobots-rasterflow MCP server in Codex at user scope (requires FLYTE_API_KEY)
	codex mcp add \
		flyte-mcp \
		--url https://$(SUBDOMAIN).apps.$(ORG).$(REGION).unionai.cloud/sdk/mcp \
		--bearer-token-env-var FLYTE_API_KEY

remove-codex-mcp: ## Remove the wherobots-rasterflow MCP server from Codex
	codex mcp remove flyte-mcp
