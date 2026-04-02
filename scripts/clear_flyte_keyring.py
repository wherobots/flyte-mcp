from __future__ import annotations

import argparse
import os
from pathlib import Path

import keyring
from flyte.config._reader import YamlConfigEntry, get_config_file
from flyte.remote._client.auth._keyring import KeyringStore, strip_scheme


def _resolve_endpoint(endpoint: str | None, config_path: str | None) -> str:
    if endpoint:
        return endpoint

    cfg_arg = None
    if config_path:
        path = Path(config_path)
        if path.exists():
            cfg_arg = path
        else:
            print(
                f"config not found at {config_path}; falling back to Flyte auto config resolution"
            )

    cfg = get_config_file(cfg_arg)
    if cfg is None:
        raise ValueError(
            "No Flyte config found. Pass --endpoint or provide --config/FLYTE_CONFIG."
        )

    resolved = cfg.get(YamlConfigEntry("admin.endpoint"))
    if not resolved:
        raise ValueError(
            "Could not read admin.endpoint from Flyte config. Pass --endpoint explicitly."
        )
    return str(resolved)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete Flyte auth tokens from keyring for an endpoint."
    )
    parser.add_argument(
        "--endpoint",
        default=None,
        help="Flyte admin endpoint (e.g. dns:///wherobots.us-west-2.unionai.cloud).",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to Flyte config.yaml. If omitted, Flyte auto config resolution is used.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be deleted without changing keyring.",
    )
    args = parser.parse_args()

    endpoint = _resolve_endpoint(args.endpoint, args.config)
    service = strip_scheme(endpoint)
    backend = keyring.get_keyring()

    print(f"endpoint={endpoint}")
    print(f"keyring_service={service}")
    print(
        f"keyring_backend={backend.__class__.__module__}.{backend.__class__.__name__}"
    )

    flyte_api_key = os.environ.get("FLYTE_API_KEY")
    if flyte_api_key:
        print("env_api_key: present")
        print(
            "warning: FLYTE_API_KEY is set in the current environment; "
            "clearing keyring will not affect API-key-based auth."
        )
    else:
        print("env_api_key: missing")

    for key in ("access_token", "refresh_token"):
        try:
            current = keyring.get_password(service, key)
            status = "present" if current else "missing"
        except Exception as e:
            status = f"unavailable ({e})"
        print(f"{key}: {status}")

    if args.dry_run:
        print("dry-run: no deletion performed")
        return

    KeyringStore.delete(endpoint)
    print("deleted keyring entries for access_token/refresh_token")


if __name__ == "__main__":
    main()
