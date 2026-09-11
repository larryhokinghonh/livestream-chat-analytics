import asyncio
import json
from collections import deque
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest


class StopRefreshLoop(Exception):
    pass


class FakeAsyncClient:
    def __init__(self, responses):
        self.responses = deque(responses)
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        return self.responses.popleft()

    async def post(self, url, **kwargs):
        self.calls.append(("POST", url, kwargs))
        return self.responses.popleft()


def response(status_code, payload=None):
    request = httpx.Request("GET", "https://api.twitch.tv/test")
    return httpx.Response(status_code, request=request, json=payload or {})


def test_healthy_token_is_not_refreshed(auth_module):
    async def scenario():
        auth_module.TOKEN_REFRESH_LOCK = asyncio.Lock()
        auth_module.validate_access_token = AsyncMock(
            return_value={"expires_in": 14_400}
        )
        auth_module.replace_access_token = AsyncMock()

        expires_in = await auth_module.ensure_valid_access_token()

        assert expires_in == 14_400
        auth_module.replace_access_token.assert_not_awaited()

    asyncio.run(scenario())


@pytest.mark.parametrize("validation", [None, {"expires_in": 300}])
def test_invalid_or_nearly_expired_token_is_refreshed(auth_module, validation):
    async def scenario():
        auth_module.TOKEN_REFRESH_LOCK = asyncio.Lock()
        auth_module.validate_access_token = AsyncMock(return_value=validation)
        auth_module.replace_access_token = AsyncMock(
            return_value={"expires_in": 14_400}
        )

        expires_in = await auth_module.ensure_valid_access_token()

        assert expires_in == 14_400
        auth_module.replace_access_token.assert_awaited_once_with()

    asyncio.run(scenario())


def test_refresh_loop_rotates_token_before_four_hour_expiry(auth_module):
    async def scenario():
        remaining = 14_400
        elapsed = 0
        refresh_requests = 0
        auth_module.TOKEN_REFRESH_LOCK = asyncio.Lock()

        async def validate():
            return {"expires_in": remaining}

        async def get_new_token():
            nonlocal refresh_requests
            refresh_requests += 1
            return {
                "access_token": "new-access-token",
                "refresh_token": "new-refresh-token",
                "expires_in": 14_400,
            }

        async def virtual_sleep(seconds):
            nonlocal elapsed, remaining
            if auth_module.ACCESS_TOKEN == "new-access-token":
                raise StopRefreshLoop
            elapsed += seconds
            remaining -= seconds

        auth_module.validate_access_token = validate
        auth_module.get_new_access_token = get_new_token
        auth_module.asyncio = type("AsyncioProxy", (), {"sleep": virtual_sleep})

        with pytest.raises(StopRefreshLoop):
            await auth_module.refresh_access_token()

        assert elapsed == 14_100
        assert remaining == auth_module.TOKEN_REFRESH_MARGIN
        assert refresh_requests == 1
        assert auth_module.ACCESS_TOKEN == "new-access-token"
        assert auth_module.REFRESH_TOKEN == "new-refresh-token"

        stored_tokens = json.loads(
            Path(auth_module.TOKEN_PATH).read_text(encoding="utf-8")
        )
        assert stored_tokens == {
            "access_token": "new-access-token",
            "refresh_token": "new-refresh-token",
        }

    asyncio.run(scenario())


def test_store_tokens_replaces_file_atomically(auth_module):
    auth_module.store_tokens("new-access", "new-refresh")

    with open(auth_module.TOKEN_PATH, encoding="utf-8") as token_file:
        assert json.load(token_file) == {
            "access_token": "new-access",
            "refresh_token": "new-refresh",
        }

    assert not auth_module.os.path.exists(f"{auth_module.TOKEN_PATH}.tmp")


def test_concurrent_401_handlers_refresh_only_once(auth_module):
    async def scenario():
        auth_module.TOKEN_REFRESH_LOCK = asyncio.Lock()

        async def replace():
            await asyncio.sleep(0)
            auth_module.ACCESS_TOKEN = "new-access-token"

        auth_module.replace_access_token = AsyncMock(side_effect=replace)

        await asyncio.gather(
            auth_module.refresh_rejected_token("old-access-token"),
            auth_module.refresh_rejected_token("old-access-token"),
        )

        auth_module.replace_access_token.assert_awaited_once_with()

    asyncio.run(scenario())


def test_stream_request_refreshes_after_401_and_uses_new_header(
    auth_module, monkeypatch
):
    async def scenario():
        auth_module.TOKEN_REFRESH_LOCK = asyncio.Lock()
        client = FakeAsyncClient([
            response(401),
            response(200, {"data": [{"id": "stream-1"}]}),
        ])
        monkeypatch.setattr(httpx, "AsyncClient", lambda: client)

        async def replace():
            auth_module.ACCESS_TOKEN = "new-access-token"

        auth_module.replace_access_token = AsyncMock(side_effect=replace)

        streams = await auth_module.check_for_online_streams(["channel-1"])

        assert streams == [{"id": "stream-1"}]
        assert [call[2]["headers"]["Authorization"] for call in client.calls] == [
            "Bearer old-access-token",
            "Bearer new-access-token",
        ]
        auth_module.replace_access_token.assert_awaited_once_with()

    asyncio.run(scenario())


def test_eventsub_request_refreshes_after_401_and_uses_new_header(
    auth_module, monkeypatch
):
    async def scenario():
        auth_module.TOKEN_REFRESH_LOCK = asyncio.Lock()
        client = FakeAsyncClient([
            response(401),
            response(202, {"data": [{"id": "subscription-1"}]}),
        ])
        monkeypatch.setattr(httpx, "AsyncClient", lambda: client)

        async def replace():
            auth_module.ACCESS_TOKEN = "new-access-token"

        auth_module.replace_access_token = AsyncMock(side_effect=replace)

        result = await auth_module.create_eventsub_subscription(
            "channel.chat.message", "channel-1", "session-1"
        )

        assert result == {"data": [{"id": "subscription-1"}]}
        assert [call[2]["headers"]["Authorization"] for call in client.calls] == [
            "Bearer old-access-token",
            "Bearer new-access-token",
        ]
        auth_module.replace_access_token.assert_awaited_once_with()

    asyncio.run(scenario())


def test_second_api_401_is_not_retried_forever(auth_module, monkeypatch):
    async def scenario():
        auth_module.TOKEN_REFRESH_LOCK = asyncio.Lock()
        client = FakeAsyncClient([response(401), response(401)])
        monkeypatch.setattr(httpx, "AsyncClient", lambda: client)

        async def replace():
            auth_module.ACCESS_TOKEN = "new-access-token"

        auth_module.replace_access_token = AsyncMock(side_effect=replace)

        with pytest.raises(httpx.HTTPStatusError):
            await auth_module.check_for_online_streams(["channel-1"])

        assert len(client.calls) == 2
        auth_module.replace_access_token.assert_awaited_once_with()

    asyncio.run(scenario())


def test_transient_refresh_failures_back_off_and_recover(auth_module):
    async def scenario():
        sleeps = []
        auth_module.ensure_valid_access_token = AsyncMock(side_effect=[
            httpx.NetworkError("network unavailable"),
            httpx.TimeoutException("request timed out"),
            14_400,
        ])

        async def fake_sleep(seconds):
            sleeps.append(seconds)
            if len(sleeps) == 3:
                raise StopRefreshLoop

        auth_module.asyncio = type("AsyncioProxy", (), {"sleep": fake_sleep})

        with pytest.raises(StopRefreshLoop):
            await auth_module.refresh_access_token()

        assert sleeps == [60, 120, 60]

    asyncio.run(scenario())


def test_invalid_refresh_credentials_require_reauthorization(auth_module):
    async def scenario():
        error_response = response(401)
        auth_module.ensure_valid_access_token = AsyncMock(
            side_effect=httpx.HTTPStatusError(
                "invalid refresh token",
                request=error_response.request,
                response=error_response,
            )
        )

        with pytest.raises(RuntimeError, match="authorization is required again"):
            await auth_module.refresh_access_token()

    asyncio.run(scenario())


def test_refresh_request_uses_current_refresh_credentials(auth_module, monkeypatch):
    async def scenario():
        client = FakeAsyncClient([
            response(200, {
                "access_token": "new-access-token",
                "refresh_token": "new-refresh-token",
                "expires_in": 14_400,
            })
        ])
        monkeypatch.setattr(httpx, "AsyncClient", lambda: client)

        tokens = await auth_module.get_new_access_token()

        assert tokens["access_token"] == "new-access-token"
        assert client.calls[0][2]["data"] == {
            "grant_type": "refresh_token",
            "refresh_token": "old-refresh-token",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
        }

    asyncio.run(scenario())
