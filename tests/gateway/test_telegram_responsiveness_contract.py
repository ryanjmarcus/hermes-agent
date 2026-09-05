"""Offline deployment gates for Telegram receipts and native choices.

Uses Hermes's real adapter/runner; the Telegram send boundary is in memory.
Xfails identify unmet migration requirements rather than report them as fixed.
"""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
import pytest
from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import MessageEvent, MessageType, SendResult
from gateway.session import SessionSource, build_session_key
from plugins.platforms.telegram.adapter import TelegramAdapter
from gateway.run import GatewayRunner
from tools import clarify_gateway


def lane():
    return SessionSource(platform=Platform.TELEGRAM, chat_id='-100999', chat_type='group', user_id='777', thread_id='12')


def event(text='Please check this', message_id='401'):
    return MessageEvent(text=text, message_type=MessageType.TEXT, source=lane(), message_id=message_id)


def adapter():
    value=TelegramAdapter(PlatformConfig(enabled=True, token='offline-token', extra={}))
    value._bot=AsyncMock()
    value._bot.send_message.return_value=SimpleNamespace(message_id=501)
    value._app=MagicMock()
    return value


def busy_runner(value, monkeypatch):
    import gateway.run as module
    monkeypatch.setattr(module, '_load_gateway_config', lambda: {})
    monkeypatch.setenv('HERMES_GATEWAY_BUSY_ACK_ENABLED','true')
    runner=object.__new__(GatewayRunner)
    runner._running_agents={};runner._running_agents_ts={};runner._pending_messages={}
    runner._busy_ack_ts={};runner._queued_events={};runner._draining=False
    runner._busy_input_mode='queue';runner._busy_text_mode='interrupt'
    runner.config=MagicMock();runner.config.group_sessions_per_user=True
    runner.config.thread_sessions_per_user=False
    runner.session_store=None;runner._is_user_authorized=lambda source: True
    runner.adapters={Platform.TELEGRAM:value}
    worker=MagicMock();worker.get_activity_summary.return_value={}
    runner._running_agents[build_session_key(lane())]=worker
    return runner,worker


@pytest.mark.asyncio
async def test_busy_worker_does_not_hold_receipt_and_topic_is_preserved(monkeypatch):
    value=adapter(); sent=[]; release=asyncio.Event()
    async def send(**kwargs):
        sent.append(kwargs);return SendResult(success=True,message_id='502')
    value._send_with_retry=send
    runner,worker=busy_runner(value,monkeypatch)
    task=asyncio.create_task(release.wait())
    try:
        assert await asyncio.wait_for(runner._handle_active_session_busy_message(event(),build_session_key(lane())),2)
        assert not task.done()
        assert len(sent)==1 and sent[0]['metadata']['thread_id']=='12'
        assert value._pending_messages[build_session_key(lane())].message_id=='401'
        worker.interrupt.assert_not_called()
    finally:
        release.set();await task


@pytest.mark.asyncio
@pytest.mark.xfail(strict=True, reason='Native busy receipts are suppressed for 30 seconds; every request receipt is not guaranteed')
async def test_each_distinct_busy_request_gets_a_receipt(monkeypatch):
    value=adapter();value._send_with_retry=AsyncMock(return_value=SendResult(success=True,message_id='502'))
    runner,_=busy_runner(value,monkeypatch)
    for number in ('401','402'):
        await runner._handle_active_session_busy_message(event(message_id=number),build_session_key(lane()))
    assert value._send_with_retry.await_count==2


@pytest.mark.asyncio
@pytest.mark.xfail(strict=True, reason='Cold native dispatch starts processing without an application-level receipt')
async def test_cold_request_has_ack_before_worker_dispatch():
    value=adapter();order=[];value._message_handler=AsyncMock()
    async def send(**kwargs):order.append('receipt');return SendResult(success=True)
    value._send_with_retry=send
    value._start_session_processing=lambda *args:order.append('worker')
    await value.handle_message(event())
    assert order[:2]==['receipt','worker']


def callback(choice_id):
    query=SimpleNamespace(data=f'cl:{choice_id}:0',
        message=SimpleNamespace(chat_id=-100999,chat=SimpleNamespace(type='supergroup'),message_thread_id=12,text='Retry job?'),
        from_user=SimpleNamespace(id=777,first_name='Tester'),answer=AsyncMock(),edit_message_text=AsyncMock())
    return SimpleNamespace(callback_query=query)


@pytest.mark.asyncio
async def test_native_choice_resolves_directly_while_worker_is_busy():
    value=adapter();key=build_session_key(lane());cid='offline-retry'
    value._is_callback_user_authorized=lambda *args,**kwargs:True
    clarify_gateway.register(cid,key,'Retry job?',['Retry','No'])
    try:
        result=await value.send_clarify('-100999','Retry job?',['Retry','No'],cid,key,metadata={'thread_id':'12'})
        assert result.success
        assert value._bot.send_message.call_args.kwargs['message_thread_id']==12
        update=callback(cid)
        await value._handle_callback_query(update,SimpleNamespace())
        entry=clarify_gateway._entries[cid]
        assert entry.response=='Retry' and entry.event.is_set()
        update.callback_query.answer.assert_awaited_once()
        # A repeated tap cannot dispatch the action a second time.
        await value._handle_callback_query(update,SimpleNamespace())
        assert 'already been resolved' in update.callback_query.answer.call_args.kwargs['text']
    finally:
        with clarify_gateway._lock:
            clarify_gateway._entries.pop(cid,None);clarify_gateway._session_index.pop(key,None)


@pytest.mark.asyncio
@pytest.mark.xfail(strict=True, reason='Native clarify routing state is adapter-local and does not survive restart')
async def test_choice_can_route_after_adapter_restart():
    first=adapter();key=build_session_key(lane());cid='offline-restart'
    clarify_gateway.register(cid,key,'Retry job?',['Retry','No'])
    try:
        await first.send_clarify('-100999','Retry job?',['Retry','No'],cid,key,metadata={'thread_id':'12'})
        second=adapter();second._is_callback_user_authorized=lambda *args,**kwargs:True
        await second._handle_callback_query(callback(cid),SimpleNamespace())
        assert clarify_gateway._entries[cid].response=='Retry'
    finally:
        with clarify_gateway._lock:
            clarify_gateway._entries.pop(cid,None);clarify_gateway._session_index.pop(key,None)


@pytest.mark.asyncio
@pytest.mark.xfail(strict=True, reason='Legacy busy_text_mode=queue bypasses native busy acknowledgment')
async def test_queue_text_mode_still_acknowledges(monkeypatch):
    value=adapter();value._send_with_retry=AsyncMock(return_value=SendResult(success=True,message_id='503'))
    runner,_=busy_runner(value,monkeypatch);runner._busy_text_mode='queue'
    await runner._handle_active_session_busy_message(event(),build_session_key(lane()))
    value._send_with_retry.assert_awaited_once()
