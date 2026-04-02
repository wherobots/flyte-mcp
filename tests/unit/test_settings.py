from flyte_mcp.settings import DiscoveryProject, MCPSettings


def test_settings_defaults():
    settings = MCPSettings(
        deployment_project="my-project",
        deployment_domain="staging",
        execution_project="my-project",
        execution_domain="staging",
    )

    assert settings.org is None
    assert settings.endpoint is None
    assert settings.execution_project == "my-project"
    assert settings.execution_domain == "staging"
    assert settings.discovery_projects == ()
    assert settings.discovery_domain == "development"
    assert settings.discovery_limit == 512
    assert settings.discovery_concurrency == 32


def test_settings_from_yaml_dict():
    settings = MCPSettings.model_validate(
        {
            "deployment_project": "my-project",
            "deployment_domain": "staging",
            "execution_project": "my-project",
            "execution_domain": "staging",
            "discovery_projects": [
                {"project": "a", "version": "latest"},
                {"project": "b", "version": "v2"},
                {"project": "c", "version": "pr-123"},
            ],
            "discovery_limit": 42,
            "org": "wherobots",
            "endpoint": "dns:///endpoint",
        }
    )

    assert settings.discovery_projects == (
        DiscoveryProject(project="a", version="latest"),
        DiscoveryProject(project="b", version="v2"),
        DiscoveryProject(project="c", version="pr-123"),
    )
    assert settings.discovery_limit == 42
    assert settings.org == "wherobots"
    assert settings.endpoint == "dns:///endpoint"
