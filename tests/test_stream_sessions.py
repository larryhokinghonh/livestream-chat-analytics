import ast
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest


@pytest.fixture
def sessions():
    path = Path(__file__).parents[1] / "script.py"
    tree = ast.parse(path.read_text())
    functions = [node for node in tree.body
                 if isinstance(node, ast.AsyncFunctionDef)
                 and node.name in {"update_online_streams_dict", "initialize_online_streams"}]
    namespace = {
        "asyncio": asyncio,
        "datetime": datetime,
        "timezone": timezone,
        "ONLINE_STREAMS": {},
        "db": SimpleNamespace(store_stream_sessions=Mock(), update_stream_sessions=Mock()),
        "check_for_online_streams": AsyncMock(),
    }
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(path), "exec"), namespace)
    return namespace


def stream(stream_id="100"):
    return {"id": stream_id, "user_id": "42", "user_name": "Channel",
            "started_at": "2026-09-06T00:00:00Z"}


def test_startup_repeat_and_offline(sessions):
    async def scenario():
        check = sessions["check_for_online_streams"]
        db = sessions["db"]
        check.return_value = [stream()]
        await sessions["initialize_online_streams"]({"channel": "42"})
        assert list(check.call_args.args[0]) == ["42"]
        db.store_stream_sessions.assert_called_once_with(
            "100", "42", "Channel", "2026-09-06T00:00:00Z")
        await sessions["update_online_streams_dict"](["42"])
        assert db.store_stream_sessions.call_count == 1
        check.return_value = []
        await sessions["update_online_streams_dict"](["42"])
        ended_id, ended_at = db.update_stream_sessions.call_args.args
        assert ended_id == "100"
        assert ended_at.tzinfo == timezone.utc
        assert sessions["ONLINE_STREAMS"] == {}
    asyncio.run(scenario())


def test_replaced_stream_closes_old_session(sessions):
    async def scenario():
        sessions["ONLINE_STREAMS"] = {"42": {"stream_id": "99"}}
        sessions["check_for_online_streams"].return_value = [stream()]
        await sessions["update_online_streams_dict"](["42"])
        assert sessions["db"].update_stream_sessions.call_args.args[0] == "99"
        assert sessions["db"].store_stream_sessions.call_args.args[0] == "100"
    asyncio.run(scenario())


def test_failed_write_preserves_snapshot(sessions):
    async def scenario():
        sessions["check_for_online_streams"].return_value = [stream()]
        sessions["db"].store_stream_sessions.side_effect = RuntimeError("write failed")
        with pytest.raises(RuntimeError, match="write failed"):
            await sessions["update_online_streams_dict"](["42"])
        assert sessions["ONLINE_STREAMS"] == {}
    asyncio.run(scenario())
