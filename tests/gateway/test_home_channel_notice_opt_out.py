"""The home setup opt-out gates only the native notice, not configured routes."""
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import Platform
from gateway.run import GatewayRunner


@pytest.mark.asyncio
@pytest.mark.parametrize("onboarding", [{}, {"home_channel_notice": True}, {"home_channel_notice": False}])
@pytest.mark.parametrize("has_home", [False, True])
async def test_home_notice_opt_out_preserves_default_and_routes(monkeypatch, onboarding, has_home):
    monkeypatch.setattr("gateway.run._load_gateway_config", lambda: {"onboarding": onboarding})
    monkeypatch.setattr("agent.secret_scope.get_secret", lambda key: "")
    monkeypatch.delenv("TELEGRAM_HOME_CHANNEL", raising=False)
    runner = GatewayRunner.__new__(GatewayRunner)
    runner.config = MagicMock()
    route = SimpleNamespace(chat_id="-1001", thread_id="10") if has_home else None
    runner.config.get_home_channel.return_value = route
    runner._deliver_platform_notice = AsyncMock()
    source = SimpleNamespace(platform=Platform.TELEGRAM, profile="default")

    await runner._maybe_deliver_home_channel_notice(source, [])

    if not has_home and onboarding.get("home_channel_notice") is not False:
        runner._deliver_platform_notice.assert_awaited_once()
        assert "/sethome" in runner._deliver_platform_notice.call_args.args[1]
    else:
        runner._deliver_platform_notice.assert_not_awaited()
    # Disabling an onboarding notice must neither invent nor clear the delivery target.
    assert runner.config.get_home_channel.return_value is route


@pytest.mark.asyncio
@pytest.mark.parametrize("platform,history", [(Platform.LOCAL, []), (Platform.WEBHOOK, []), (Platform.TELEGRAM, [{"role": "user"}])])
async def test_existing_notice_exclusions_remain(monkeypatch, platform, history):
    monkeypatch.setattr("gateway.run._load_gateway_config", lambda: {})
    runner = GatewayRunner.__new__(GatewayRunner)
    runner.config = MagicMock()
    runner._deliver_platform_notice = AsyncMock()

    await runner._maybe_deliver_home_channel_notice(SimpleNamespace(platform=platform), history)

    runner._deliver_platform_notice.assert_not_awaited()
    runner.config.get_home_channel.assert_not_called()
