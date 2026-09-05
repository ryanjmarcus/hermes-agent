"""Exercise the native Telegram handler against Hermes's real SQLite job ledger."""
import asyncio
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
import threading
import pytest
from tools import async_delegation as ad
from gateway.config import PlatformConfig
from plugins.platforms.telegram.adapter import TelegramAdapter

spec=importlib.util.spec_from_file_location('job_controls',Path(__file__).resolve().parents[2]/'hermes_migration/telegram-job-controls/__init__.py')
jobs=importlib.util.module_from_spec(spec);spec.loader.exec_module(jobs)


def make_adapter():
    value=TelegramAdapter(PlatformConfig(enabled=True,token='offline-token',extra={}))
    value._is_callback_user_authorized=lambda *args,**kwargs:True
    return value


def native_record(at=1234):
    ad._persist_dispatch({'delegation_id':'offline-job','session_key':'telegram-topic-12','dispatched_at':at,'goal':'fixture'})
    return ad.get_durable_delegation('offline-job')


def issue(store,job):
    return store.issue(job=job,owner_id='777',chat_id='-100999',thread_id='12',message_id='501')


def update(token,**overrides):
    args={'owner_id':777,'chat_id':-100999,'thread_id':12,'message_id':501};args.update(overrides)
    query=SimpleNamespace(data='jobs:p:'+token,from_user=SimpleNamespace(id=args['owner_id'],first_name='Tester'),
        message=SimpleNamespace(chat_id=args['chat_id'],message_id=args['message_id'],message_thread_id=args['thread_id'],chat=SimpleNamespace(type='supergroup')),
        answer=AsyncMock(),edit_message_text=AsyncMock())
    return SimpleNamespace(callback_query=query)


@pytest.mark.asyncio
async def test_persisted_button_survives_adapter_reconstruction_and_is_read_only(tmp_path):
    store=jobs.Bindings(tmp_path/'buttons.db');record=native_record();token=issue(store,record)
    controller=jobs.Controller(make_adapter(),jobs.Bindings(tmp_path/'buttons.db'))
    tapped=update(token)
    await controller.handle(tapped,None)
    assert 'State: running' in tapped.callback_query.edit_message_text.call_args.args[0]
    assert ad.get_durable_delegation('offline-job')==record
    # Repeated Details/Probe clicks only read; they never spawn/retry a worker.
    await controller.handle(tapped,None)
    assert ad.get_durable_delegation('offline-job')==record
    assert len('jobs:p:'+token)<=64


@pytest.mark.asyncio
@pytest.mark.parametrize('wrong',[{'owner_id':778},{'chat_id':-100888},{'thread_id':13},{'message_id':502}])
async def test_scope_mismatch_never_reads_or_discloses_job(tmp_path,wrong):
    store=jobs.Bindings(tmp_path/'buttons.db');token=issue(store,native_record())
    reader=MagicMock(side_effect=AssertionError('must not read another scope'))
    controller=jobs.Controller(make_adapter(),store,read_job=reader)
    tapped=update(token,**wrong);await controller.handle(tapped,None)
    reader.assert_not_called();tapped.callback_query.edit_message_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_changed_attempt_rejects_old_button(tmp_path):
    store=jobs.Bindings(tmp_path/'buttons.db');token=issue(store,native_record())
    native_record(at=9999)
    tapped=update(token);await jobs.Controller(make_adapter(),store).handle(tapped,None)
    tapped.callback_query.edit_message_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_authorization_is_rechecked_on_tap(tmp_path):
    store=jobs.Bindings(tmp_path/'buttons.db');token=issue(store,native_record());adapter=make_adapter()
    adapter._is_callback_user_authorized=lambda *args,**kwargs:False
    reader=MagicMock();tapped=update(token)
    await jobs.Controller(adapter,store,reader).handle(tapped,None)
    reader.assert_not_called();assert 'not authorized' in tapped.callback_query.answer.call_args.args[0]


@pytest.mark.asyncio
async def test_slow_probe_answers_callback_first_and_never_multiplies_readers(tmp_path):
    store=jobs.Bindings(tmp_path/'buttons.db');record=native_record();token=issue(store,record)
    release=threading.Event();started=threading.Event();calls=[];tapped=update(token)
    def slow(*args,**kwargs):
        assert tapped.callback_query.answer.await_count==1
        calls.append(1);started.set();release.wait(2);return record
    controller=jobs.Controller(make_adapter(),store,slow,read_budget=.03)
    try:
        await controller.handle(tapped,None)
        assert started.is_set() and 'has not returned' in tapped.callback_query.answer.call_args.args[0]
        for _ in range(3):await controller.handle(update(token),None)
        assert calls==[1]
    finally:
        release.set()
        if controller._read:await controller._read


def test_register_uses_native_pattern_scoped_factory():
    ctx=MagicMock();jobs.register(ctx)
    factory=ctx.register_telegram_handler.call_args.args[0]
    app=MagicMock();factory(app,make_adapter())
    handler=app.add_handler.call_args.args[0]
    assert handler.pattern.match('jobs:p:abc')
    assert not handler.pattern.match('ea:once:5')
    assert not handler.pattern.match('cl:question:0')


@pytest.mark.asyncio
async def test_attach_requires_real_job_same_origin_and_keeps_message_anchor(tmp_path):
    adapter=make_adapter();adapter._bot=AsyncMock();native_record()
    store=jobs.Bindings(tmp_path/'buttons.db')
    receipt=await jobs.attach_controls(adapter,job_id='offline-job',owner_id='777',chat_id='-100999',thread_id='12',message_id='501',origin_session='telegram-topic-12',bindings=store)
    assert receipt['attempt']==1234
    assert adapter._bot.edit_message_reply_markup.call_args.kwargs['message_id']==501
    with pytest.raises(ValueError):
        await jobs.attach_controls(adapter,job_id='offline-job',owner_id='777',chat_id='-100999',thread_id='12',message_id='501',origin_session='wrong-topic',bindings=store)
    assert adapter._bot.edit_message_reply_markup.await_count==1
