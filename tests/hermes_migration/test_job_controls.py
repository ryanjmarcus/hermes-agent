"""Exercise the native Telegram handler against Hermes's real SQLite job ledger."""

import asyncio
import json
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
import threading
import pytest
from tools import async_delegation as ad
from gateway.config import PlatformConfig
from plugins.platforms.telegram.adapter import TelegramAdapter

spec = importlib.util.spec_from_file_location(
    "job_controls",
    Path(__file__).resolve().parents[2]
    / "hermes_migration/telegram-job-controls/__init__.py",
)
jobs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(jobs)


def make_adapter():
    value = TelegramAdapter(
        PlatformConfig(enabled=True, token="offline-token", extra={})
    )
    value._is_callback_user_authorized = lambda *args, **kwargs: True
    return value


def native_record(at=1234):
    ad._persist_dispatch({
        "delegation_id": "offline-job",
        "session_key": "telegram-topic-12",
        "dispatched_at": at,
        "goal": "fixture",
    })
    return ad.get_durable_delegation("offline-job")


def issue(store, job):
    return store.issue(
        job=job, owner_id="777", chat_id="-100999", thread_id="12", message_id="501"
    )


def update(token, **overrides):
    args = {"owner_id": 777, "chat_id": -100999, "thread_id": 12, "message_id": 501}
    args.update(overrides)
    query = SimpleNamespace(
        data="jobs:p:" + token,
        from_user=SimpleNamespace(id=args["owner_id"], first_name="Tester"),
        message=SimpleNamespace(
            chat_id=args["chat_id"],
            message_id=args["message_id"],
            message_thread_id=args["thread_id"],
            chat=SimpleNamespace(type="supergroup"),
        ),
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
    )
    return SimpleNamespace(callback_query=query)


@pytest.mark.asyncio
async def test_persisted_button_survives_adapter_reconstruction_and_is_read_only(
    tmp_path,
):
    store = jobs.Bindings(tmp_path / "buttons.db")
    record = native_record()
    token = issue(store, record)
    controller = jobs.Controller(make_adapter(), jobs.Bindings(tmp_path / "buttons.db"))
    tapped = update(token)
    await controller.handle(tapped, None)
    assert "State: running" in tapped.callback_query.edit_message_text.call_args.args[0]
    assert ad.get_durable_delegation("offline-job") == record
    # Repeated Details/Probe clicks only read; they never spawn/retry a worker.
    await controller.handle(tapped, None)
    assert ad.get_durable_delegation("offline-job") == record
    assert len("jobs:p:" + token) <= 64


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "wrong",
    [{"owner_id": 778}, {"chat_id": -100888}, {"thread_id": 13}, {"message_id": 502}],
)
async def test_scope_mismatch_never_reads_or_discloses_job(tmp_path, wrong):
    store = jobs.Bindings(tmp_path / "buttons.db")
    token = issue(store, native_record())
    reader = MagicMock(side_effect=AssertionError("must not read another scope"))
    controller = jobs.Controller(make_adapter(), store, read_job=reader)
    tapped = update(token, **wrong)
    await controller.handle(tapped, None)
    reader.assert_not_called()
    tapped.callback_query.edit_message_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_changed_attempt_rejects_old_button(tmp_path):
    store = jobs.Bindings(tmp_path / "buttons.db")
    token = issue(store, native_record())
    native_record(at=9999)
    tapped = update(token)
    await jobs.Controller(make_adapter(), store).handle(tapped, None)
    tapped.callback_query.edit_message_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_authorization_is_rechecked_on_tap(tmp_path):
    store = jobs.Bindings(tmp_path / "buttons.db")
    token = issue(store, native_record())
    adapter = make_adapter()
    adapter._is_callback_user_authorized = lambda *args, **kwargs: False
    reader = MagicMock()
    tapped = update(token)
    await jobs.Controller(adapter, store, reader).handle(tapped, None)
    reader.assert_not_called()
    assert "not authorized" in tapped.callback_query.answer.call_args.args[0]


@pytest.mark.asyncio
async def test_slow_probe_answers_callback_first_and_never_multiplies_readers(tmp_path):
    store = jobs.Bindings(tmp_path / "buttons.db")
    record = native_record()
    token = issue(store, record)
    release = threading.Event()
    started = threading.Event()
    calls = []
    tapped = update(token)

    def slow(*args, **kwargs):
        assert tapped.callback_query.answer.await_count == 1
        calls.append(1)
        started.set()
        release.wait(2)
        return record

    controller = jobs.Controller(make_adapter(), store, slow, read_budget=0.03)
    try:
        await controller.handle(tapped, None)
        assert (
            started.is_set()
            and "has not returned" in tapped.callback_query.answer.call_args.args[0]
        )
        for _ in range(3):
            await controller.handle(update(token), None)
        assert calls == [1]
    finally:
        release.set()
        if controller._read:
            await controller._read


@pytest.mark.asyncio
async def test_register_uses_native_pattern_scoped_factory():
    ctx = MagicMock()
    pending = []
    ctx.spawn_task.side_effect = lambda coro, **kw: pending.append(
        asyncio.create_task(coro)
    )
    jobs.register(ctx)
    factory = ctx.register_telegram_handler.call_args.args[0]
    app = MagicMock()
    factory(app, make_adapter())
    handler = app.add_handler.call_args.args[0]
    assert handler.pattern.match("jobs:p:abc")
    assert not handler.pattern.match("ea:once:5")
    assert not handler.pattern.match("cl:question:0")
    await asyncio.gather(*pending)


@pytest.mark.asyncio
async def test_attach_requires_real_job_same_origin_and_keeps_message_anchor(tmp_path):
    adapter = make_adapter()
    adapter._bot = AsyncMock()
    native_record()
    store = jobs.Bindings(tmp_path / "buttons.db")
    receipt = await jobs.attach_controls(
        adapter,
        job_id="offline-job",
        owner_id="777",
        chat_id="-100999",
        thread_id="12",
        message_id="501",
        origin_session="telegram-topic-12",
        bindings=store,
    )
    assert receipt["attempt"] == 1234
    assert adapter._bot.edit_message_reply_markup.call_args.kwargs["message_id"] == 501
    with pytest.raises(ValueError):
        await jobs.attach_controls(
            adapter,
            job_id="offline-job",
            owner_id="777",
            chat_id="-100999",
            thread_id="12",
            message_id="501",
            origin_session="wrong-topic",
            bindings=store,
        )
    assert adapter._bot.edit_message_reply_markup.await_count == 1


class PluginContext:
    def __init__(self):
        self.tasks = []

    def spawn_task(self, coro, *, name):
        task = asyncio.create_task(coro, name=name)
        self.tasks.append(task)
        return task


@pytest.mark.asyncio
async def test_accepted_native_dispatch_automatically_posts_one_goal_card_with_real_receipt():
    from gateway.session_context import set_session_vars, clear_session_vars
    from gateway.platforms.base import SendResult

    adapter = make_adapter()
    adapter._bot = AsyncMock()
    adapter.send = AsyncMock(return_value=SendResult(success=True, message_id="921"))
    ctx = PluginContext()
    cards = jobs.AutomaticCards(ctx)
    cards.connected(adapter)
    release = threading.Event()

    def work():
        release.wait(3)
        return {"summary": "done"}

    tokens = set_session_vars(
        platform="telegram",
        chat_id="-100999",
        chat_type="group",
        thread_id="12",
        user_id="777",
        session_key="telegram-topic-12",
    )
    try:
        dispatch = ad.dispatch_async_delegation_batch(
            goals=["Fix the documented sale copy"],
            context=None,
            toolsets=None,
            role="coder",
            model=None,
            session_key="telegram-topic-12",
            runner=work,
        )
        assert dispatch["status"] == "dispatched"
        payload = {
            **dispatch,
            "mode": "background",
            "goals": ["Fix the documented sale copy"],
        }
        await asyncio.to_thread(cards.post_tool, "delegate_task", json.dumps(payload))
        await asyncio.to_thread(cards.post_tool, "delegate_task", json.dumps(payload))
        # Scheduling occurs on the captured native Telegram loop, not tool threads.
        await asyncio.sleep(0)
        await asyncio.gather(*ctx.tasks)
        assert adapter.send.await_count == 1
        assert "Fix the documented sale copy" in adapter.send.call_args.args[1]
        assert dispatch["delegation_id"] in adapter.send.call_args.args[1]
        assert adapter.send.call_args.kwargs["metadata"] == {"thread_id": "12"}
        assert (
            adapter._bot.edit_message_reply_markup.call_args.kwargs["message_id"] == 921
        )
        markup = adapter._bot.edit_message_reply_markup.call_args.kwargs[
            "reply_markup"
        ].to_dict()
        token = markup["inline_keyboard"][0][0]["callback_data"].split(":")[2]
        binding = jobs.Bindings().get(token)
        assert (
            binding["message_id"] == "921"
            and binding["title"] == "Fix the documented sale copy"
        )
        assert binding["job_id"] == dispatch["delegation_id"]
    finally:
        clear_session_vars(tokens)
        release.set()


@pytest.mark.asyncio
async def test_rejected_or_unbound_dispatch_cannot_post_using_environment_destination(
    monkeypatch,
):
    from gateway.session_context import reset_session_vars

    reset_session_vars()
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "telegram")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "-100999")
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "777")
    monkeypatch.setenv("HERMES_SESSION_KEY", "telegram-topic-12")
    ctx = PluginContext()
    cards = jobs.AutomaticCards(ctx)
    cards.connected(make_adapter())
    cards.post_tool(
        "delegate_task",
        {
            "status": "rejected",
            "delegation_id": "made-up",
            "mode": "background",
            "goals": ["wrong"],
        },
    )
    cards.post_tool(
        "delegate_task",
        {
            "status": "dispatched",
            "delegation_id": "made-up",
            "mode": "background",
            "goals": ["wrong"],
        },
    )
    await asyncio.sleep(0)
    await asyncio.gather(*ctx.tasks)
    assert all(task.get_name() == "telegram-job-card-reconnect" for task in ctx.tasks)


@pytest.mark.asyncio
async def test_unconfirmed_send_does_not_attach_controls_or_retry_blindly():
    from gateway.platforms.base import SendResult

    native_record()
    adapter = make_adapter()
    adapter._bot = AsyncMock()
    adapter.send = AsyncMock(return_value=SendResult(success=False, error="timeout"))
    cards = jobs.AutomaticCards(PluginContext())
    source = {
        "platform": "telegram",
        "chat_id": "-100999",
        "chat_type": "group",
        "thread_id": "12",
        "user_id": "777",
        "session_key": "telegram-topic-12",
        "profile": "",
    }
    payload = {"delegation_id": "offline-job", "goals": ["Fix the sale copy"]}
    await cards.publish(adapter, source, payload)
    await cards.publish(adapter, source, payload)
    assert adapter.send.await_count == 1
    adapter._bot.edit_message_reply_markup.assert_not_awaited()


def test_details_preserve_goal_native_timestamps_and_result():
    output = jobs.render(
        {
            "delegation_id": "x",
            "state": "completed",
            "title": "Fix the sale copy",
            "dispatched_at": 1234,
            "completed_at": 1240,
            "delivery_state": "delivered",
            "delivery_attempts": 1,
            "result": {"summary": "Published verified PR"},
        },
        probe=False,
    )
    for expected in (
        "Fix the sale copy",
        "Started:",
        "Completed:",
        "Published verified PR",
        "delivered",
    ):
        assert expected in output


def card_fixture(store, record):
    source = dict(
        platform="telegram",
        profile="",
        chat_id="-100999",
        chat_type="supergroup",
        thread_id="12",
        user_id="777",
        session_key="telegram-topic-12",
    )
    key = store.claim_card(record, source)
    token = issue(store, {**record, "title": "Fix verified sale copy"})
    store.record_card(
        key,
        "controls_attached",
        message_id="501",
        token=token,
        title="Fix verified sale copy",
    )
    return key


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,batch", [("completed", False), ("error", True), ("cancelled", False)]
)
async def test_native_completion_observer_updates_confirmed_card_without_consuming_delivery(
    monkeypatch, status, batch
):
    from hermes_cli import plugins

    store = jobs.Bindings()
    record = native_record()
    card_fixture(store, record)
    adapter = make_adapter()
    adapter._bot = AsyncMock()
    ctx = PluginContext()
    cards = jobs.AutomaticCards(ctx)
    cards.connected(adapter)
    await asyncio.gather(*ctx.tasks)
    observed = []

    def observe(name, *, event):
        assert name == "on_async_delegation_completed"
        assert ad.get_durable_delegation(event["delegation_id"])["state"] == status
        observed.append(event)
        cards.completed(event)

    monkeypatch.setattr(
        plugins, "has_hook", lambda name: name == "on_async_delegation_completed"
    )
    monkeypatch.setattr(plugins, "invoke_hook", observe)
    native = {
        "delegation_id": record["delegation_id"],
        "session_key": record["origin_session"],
        "dispatched_at": record["dispatched_at"],
        "completed_at": 1300,
        "goal": "fixture",
    }
    result = (
        {"results": [{"summary": "Verified result preserved"}]}
        if batch
        else {"summary": "Verified result preserved"}
    )
    push = ad._push_batch_completion_event if batch else ad._push_completion_event
    await asyncio.to_thread(push, native, result, status)
    await asyncio.sleep(0)
    await asyncio.gather(*ctx.tasks)
    assert len(observed) == 1
    args = adapter._bot.edit_message_text.call_args.kwargs
    assert args["chat_id"] == -100999 and args["message_id"] == 501
    assert (
        "Fix verified sale copy" in args["text"]
        and "Verified result preserved" in args["text"]
    )
    saved = ad.get_durable_delegation(record["delegation_id"])
    assert saved["delivery_state"] == "pending" and saved["delivery_attempts"] == 0
    assert saved["result"] == result
    # Duplicate notification and reconstructed plugin cannot edit the card again.
    await cards.reconcile(adapter, observed[0])
    restarted = jobs.AutomaticCards(PluginContext())
    await restarted.reconcile(adapter, observed[0])
    assert adapter._bot.edit_message_text.await_count == 1


@pytest.mark.asyncio
async def test_old_attempt_or_late_success_cannot_overwrite_cancelled_card():
    store = jobs.Bindings()
    record = native_record()
    card_fixture(store, record)
    adapter = make_adapter()
    adapter._bot = AsyncMock()
    cards = jobs.AutomaticCards(PluginContext())
    event = dict(
        delegation_id="offline-job",
        session_key="telegram-topic-12",
        dispatched_at=1234,
        completed_at=1300,
        status="cancelled",
    )
    ad._persist_completion(event, {"summary": "Cancelled"})
    await cards.reconcile(adapter, {**event, "dispatched_at": 1220})
    adapter._bot.edit_message_text.assert_not_awaited()
    await cards.reconcile(adapter, event)
    # Even a late native write cannot overwrite the already-confirmed terminal card.
    ad._persist_completion({**event, "status": "completed"}, {"summary": "late result"})
    await cards.reconcile(adapter, {**event, "status": "completed"})
    assert adapter._bot.edit_message_text.await_count == 1
    assert "cancelled" in adapter._bot.edit_message_text.call_args.kwargs["text"]


@pytest.mark.asyncio
async def test_failed_terminal_edit_preserves_native_result_for_details_and_no_blind_replay():
    store = jobs.Bindings()
    record = native_record()
    card_fixture(store, record)
    adapter = make_adapter()
    adapter._bot = AsyncMock()
    adapter._bot.edit_message_text.side_effect = TimeoutError("offline transport")
    event = dict(
        delegation_id="offline-job",
        session_key="telegram-topic-12",
        dispatched_at=1234,
        completed_at=1300,
        status="completed",
    )
    ad._persist_completion(event, {"summary": "PR result remains available"})
    before = ad.get_durable_delegation("offline-job")
    cards = jobs.AutomaticCards(PluginContext())
    await cards.reconcile(adapter, event)
    await jobs.AutomaticCards(PluginContext()).reconcile(adapter, event)
    assert adapter._bot.edit_message_text.await_count == 1
    assert ad.get_durable_delegation("offline-job") == before
    receipt = store.confirmed_cards()[0][1]
    assert receipt["terminal_edit"] == "unconfirmed"
    tapped = update(receipt["token"])
    await jobs.Controller(adapter, store).handle(tapped, None)
    assert (
        "PR result remains available"
        in tapped.callback_query.edit_message_text.call_args.args[0]
    )


@pytest.mark.asyncio
async def test_reconnect_catches_completion_missed_while_adapter_absent():
    store = jobs.Bindings()
    record = native_record()
    card_fixture(store, record)
    ad._persist_completion(
        dict(delegation_id="offline-job", status="completed", completed_at=1300),
        {"summary": "done"},
    )
    adapter = make_adapter()
    adapter._bot = AsyncMock()
    ctx = PluginContext()
    jobs.AutomaticCards(ctx).connected(adapter)
    await asyncio.gather(*ctx.tasks)
    assert adapter._bot.edit_message_text.await_count == 1
