import ast
import asyncio
import json
import os
from pathlib import Path
import httpx
import pytest


AUTH_FUNCTIONS = {
    "check_for_online_streams",
    "create_eventsub_subscription",
    "ensure_valid_access_token",
    "get_new_access_token",
    "refresh_access_token",
    "refresh_rejected_token",
    "replace_access_token",
    "store_tokens",
    "validate_access_token",
}


class ScriptNamespace:
    def __init__(self, namespace):
        object.__setattr__(self, "_namespace", namespace)

    def __getattr__(self, name):
        return self._namespace[name]

    def __setattr__(self, name, value):
        self._namespace[name] = value


@pytest.fixture
def auth_module(tmp_path):
    script_path = Path(__file__).parents[1] / "script.py"
    syntax_tree = ast.parse(script_path.read_text(encoding="utf-8"))
    functions = [
        node
        for node in syntax_tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in AUTH_FUNCTIONS
    ]

    namespace = {
        "asyncio": asyncio,
        "json": json,
        "os": os,
        "httpx": httpx,
        "ACCESS_TOKEN": "old-access-token",
        "REFRESH_TOKEN": "old-refresh-token",
        "CLIENT_ID": "test-client-id",
        "CLIENT_SECRET": "test-client-secret",
        "USER_ID": "test-user-id",
        "TOKEN_PATH": str(tmp_path / "twitch_tokens.json"),
        "TOKEN_REFRESH_MARGIN": 5 * 60,
        "TOKEN_VALIDATION_INTERVAL": 60,
        "TOKEN_RETRY_DELAY": 60,
        "TOKEN_REFRESH_LOCK": asyncio.Lock(),
    }

    module = ast.Module(body=functions, type_ignores=[])
    exec(compile(module, str(script_path), "exec"), namespace)

    return ScriptNamespace(namespace)
