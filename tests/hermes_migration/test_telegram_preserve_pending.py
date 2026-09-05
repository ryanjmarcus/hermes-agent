"""Actual adapter startup/recovery calls, with Telegram transport kept offline."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from gateway.config import PlatformConfig, Platform, load_gateway_config
from plugins.platforms.telegram.adapter import TelegramAdapter


async def cleanup(adapter):
    tasks = set(adapter._background_tasks)
    for name in (
        "_polling_heartbeat_task",
        "_bot_identity_refresh_task",
        "_post_connect_task",
    ):
        task = getattr(adapter, name, None)
        if isinstance(task, asyncio.Task):
            tasks.add(task)
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def wire(monkeypatch, adapter, *, webhook, progress=True):
    import plugins.platforms.telegram.adapter as module

    captured = {}

    async def start_polling(**kwargs):
        captured["polling"] = kwargs
        if progress:
            adapter._record_polling_progress(adapter._polling_generation)

    async def start_webhook(**kwargs):
        captured["webhook"] = kwargs

    updater = SimpleNamespace(
        start_polling=AsyncMock(side_effect=start_polling),
        start_webhook=AsyncMock(side_effect=start_webhook),
        stop=AsyncMock(),
        running=True,
    )
    bot = SimpleNamespace(
        set_my_commands=AsyncMock(), delete_webhook=AsyncMock(), username="offline_bot"
    )
    app = SimpleNamespace(
        bot=bot,
        updater=updater,
        add_handler=MagicMock(),
        initialize=AsyncMock(),
        start=AsyncMock(),
        shutdown=AsyncMock(),
    )
    builder = MagicMock()
    builder.token.return_value = builder
    builder.request.return_value = builder
    builder.get_updates_request.return_value = builder
    builder.build.return_value = app
    monkeypatch.setattr(module, "Application", SimpleNamespace(builder=lambda: builder))
    monkeypatch.setattr(module, "discover_fallback_ips", AsyncMock(return_value=[]))
    monkeypatch.setattr(module, "HTTPXRequest", lambda **kwargs: MagicMock())
    monkeypatch.setattr(
        "gateway.status.acquire_scoped_lock", lambda *args, **kwargs: (True, None)
    )
    monkeypatch.setattr(
        "gateway.status.release_scoped_lock", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(adapter, "_start_post_connect_housekeeping", lambda: None)
    if webhook:
        monkeypatch.setenv("TELEGRAM_WEBHOOK_URL", "https://offline.invalid/telegram")
        monkeypatch.setenv("TELEGRAM_WEBHOOK_SECRET", "offline-fixture-secret")
    else:
        monkeypatch.delenv("TELEGRAM_WEBHOOK_URL", raising=False)
    original_start = adapter._start_polling_resilient
    original_delete = adapter._delete_webhook_best_effort

    async def polling_policy(**kwargs):
        captured["policy"] = kwargs
        return await original_start(**kwargs)

    async def delete_policy(**kwargs):
        captured["deletion_policy"] = kwargs
        return await original_delete(**kwargs)

    monkeypatch.setattr(adapter, "_start_polling_resilient", polling_policy)
    monkeypatch.setattr(adapter, "_delete_webhook_best_effort", delete_policy)
    return captured, bot


@pytest.mark.asyncio
@pytest.mark.parametrize("webhook", [False, True])
@pytest.mark.parametrize("reconnect", [False, True])
@pytest.mark.parametrize("preserve", [False, True])
async def test_actual_connect_queue_policy_and_readiness(
    monkeypatch, webhook, reconnect, preserve
):
    adapter = TelegramAdapter(
        PlatformConfig(
            enabled=True,
            token="offline-token",
            extra={"preserve_pending_updates": preserve},
        )
    )
    captured, bot = wire(monkeypatch, adapter, webhook=webhook)
    try:
        assert await adapter.connect(is_reconnect=reconnect)
        transport = captured["webhook" if webhook else "polling"]
        assert transport["drop_pending_updates"] is (not reconnect and not preserve)
        if not webhook:
            assert captured["policy"]["require_progress"] is (not reconnect)
            assert captured["deletion_policy"]["require_success"] is (not reconnect)
            bot.delete_webhook.assert_awaited_once_with(drop_pending_updates=False)
        else:
            assert transport["secret_token"] == "offline-fixture-secret"
    finally:
        await cleanup(adapter)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "extra, expected",
    [
        ({}, True),
        ({"preserve_pending_updates": True}, False),
        ({"preserve_pending_updates": "false"}, True),
        ({"preserve_pending_updates": "true"}, False),
    ],
)
async def test_conflict_retry_obeys_preservation_policy(monkeypatch, extra, expected):
    adapter = TelegramAdapter(
        PlatformConfig(enabled=True, token="offline-token", extra=extra)
    )
    adapter.set_fatal_error_handler(AsyncMock())
    monkeypatch.setattr(adapter, "_drain_polling_connections", AsyncMock())
    captured = {}

    async def start_polling(**kwargs):
        captured.update(kwargs)

    adapter._app = SimpleNamespace(
        updater=SimpleNamespace(
            start_polling=AsyncMock(side_effect=start_polling),
            stop=AsyncMock(),
            running=True,
        )
    )
    original_sleep = asyncio.sleep

    async def skip_retry_delay(seconds):
        await original_sleep(0 if seconds >= 10 else seconds)

    monkeypatch.setattr(asyncio, "sleep", skip_retry_delay)
    conflict = type("Conflict", (Exception,), {})
    try:
        await adapter._handle_polling_conflict(
            conflict("Conflict: terminated by other getUpdates request")
        )
        assert captured["drop_pending_updates"] is expected
        assert adapter._polling_conflict_count == 1
        assert not adapter.has_fatal_error
    finally:
        await cleanup(adapter)


@pytest.mark.asyncio
async def test_preserve_enabled_still_rejects_cold_start_without_polling_progress(
    monkeypatch,
):
    import plugins.platforms.telegram.adapter as module

    adapter = TelegramAdapter(
        PlatformConfig(
            enabled=True,
            token="offline-token",
            extra={"preserve_pending_updates": True},
        )
    )
    captured, _ = wire(monkeypatch, adapter, webhook=False, progress=False)
    monkeypatch.setattr(module, "_INITIAL_POLLING_PROGRESS_TIMEOUT", 0.03)
    try:
        assert not await adapter.connect()
        assert captured["polling"]["drop_pending_updates"] is False
        assert captured["policy"]["require_progress"] is True
        assert adapter.has_fatal_error
    finally:
        await cleanup(adapter)


def test_documented_config_reaches_actual_adapter(monkeypatch):
    from hermes_constants import get_hermes_home

    home = get_hermes_home()
    home.mkdir(parents=True, exist_ok=True)
    (home / "config.yaml").write_text(
        "platforms:\n  telegram:\n    enabled: true\n    extra:\n      preserve_pending_updates: true\n"
    )
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "offline-token")
    config = load_gateway_config()
    adapter = TelegramAdapter(config.platforms[Platform.TELEGRAM])
    assert adapter._should_drop_pending_updates() is False
    assert adapter._should_drop_pending_updates(is_reconnect=True) is False
