"""Native Telegram job inspection; no worker execution, generic receipts or polling."""
import asyncio
import json
from pathlib import Path
import secrets
import sqlite3
import time
from contextlib import contextmanager


class Bindings:
    """Only callback routing is stored here; native Hermes owns job state."""
    def __init__(self, path=None):
        if path is None:
            from hermes_constants import get_hermes_home
            path=get_hermes_home()/'plugins/telegram-job-controls/buttons.db'
        self.path=Path(path);self.path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        with self.connect() as conn:
            conn.execute('CREATE TABLE IF NOT EXISTS buttons (token TEXT PRIMARY KEY, binding TEXT NOT NULL)')
        self.path.chmod(0o600)

    @contextmanager
    def connect(self):
        conn=sqlite3.connect(self.path,timeout=1)
        try:
            with conn:yield conn
        finally:conn.close()

    def issue(self, *, job, owner_id, chat_id, thread_id, message_id):
        for field in ('delegation_id','dispatched_at','origin_session'):
            if not job.get(field):raise ValueError('Job must exist before issuing controls: '+field)
        token=secrets.token_urlsafe(16)
        binding={'job_id':job['delegation_id'],'attempt':job['dispatched_at'],'origin_session':job['origin_session'],
                 'owner_id':str(owner_id),'chat_id':str(chat_id),'thread_id':str(thread_id or ''),
                 'message_id':str(message_id),'created_at':time.time()}
        with self.connect() as conn:
            conn.execute('INSERT INTO buttons VALUES (?,?)',(token,json.dumps(binding)))
        return token

    def get(self, token):
        with self.connect() as conn:
            row=conn.execute('SELECT binding FROM buttons WHERE token=?',(token,)).fetchone()
        return json.loads(row[0]) if row else None


def native_job(job_id, probe=False):
    from tools.async_delegation import get_durable_delegation, list_async_delegations
    job=get_durable_delegation(job_id)
    if job and probe:
        current=next((row for row in list_async_delegations() if row.get('delegation_id')==job_id),None)
        job['live_registry_present']=current is not None
        if current:
            for key in ('seconds_since_progress','children_activity','in_tool'):
                if key in current:job[key]=current[key]
    return job


def keyboard(token):
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    return InlineKeyboardMarkup([[InlineKeyboardButton('Details',callback_data='jobs:d:'+token),
                                  InlineKeyboardButton('Probe',callback_data='jobs:p:'+token)]])


async def attach_controls(adapter, *, job_id, owner_id, chat_id, thread_id, message_id, origin_session, bindings=None):
    """Called by the coordinator only after a real job and its card exist."""
    job=await asyncio.to_thread(native_job,job_id)
    if not job or job.get('origin_session')!=origin_session:
        raise ValueError('Job does not belong to the coordinator session')
    store=bindings or Bindings()
    token=await asyncio.to_thread(store.issue,job=job,owner_id=owner_id,chat_id=chat_id,
                                  thread_id=thread_id,message_id=message_id)
    await adapter._bot.edit_message_reply_markup(chat_id=int(chat_id),message_id=int(message_id),reply_markup=keyboard(token))
    return {'job_id':job_id,'attempt':job['dispatched_at'],'message_id':str(message_id)}


def render(job, probe=False):
    text=f"Job {job['delegation_id']}\nState: {job['state']}\nDelivery: {job.get('delivery_state','unknown')}"
    if probe:
        text+='\nLive registry: '+('present' if job.get('live_registry_present') else 'not present')
        if 'seconds_since_progress' in job:text+=f"\nSeconds since progress: {job['seconds_since_progress']}"
        if 'in_tool' in job:text+=f"\nTool active: {bool(job['in_tool'])}"
        text+='\nProbe does not restart or duplicate work.'
        if job['state']=='unknown':text+='\nThe prior owner exited without a confirmed result; inspect artifacts before retrying.'
    return text


class Controller:
    def __init__(self, adapter, bindings=None, read_job=native_job, read_budget=2):
        self.adapter=adapter;self.bindings=bindings or Bindings();self.read_job=read_job
        self.read_budget=read_budget;self._read=None

    def _resolve(self, token, scope, probe):
        binding=self.bindings.get(token)
        if not binding or any(binding.get(k)!=v for k,v in scope.items()):return None
        job=self.read_job(binding['job_id'],probe=probe)
        if not job or job.get('dispatched_at')!=binding['attempt'] or job.get('origin_session')!=binding['origin_session']:return None
        return job

    async def handle(self, update, context):
        query=update.callback_query
        data=getattr(query,'data','') or ''
        parts=data.split(':')
        if len(parts)!=3 or parts[0]!='jobs' or parts[1] not in ('d','p'):return
        message=getattr(query,'message',None);user=getattr(query,'from_user',None)
        if not message or not user:
            await query.answer('This job control is unavailable.');return
        chat=getattr(message,'chat',None)
        scope={'owner_id':str(user.id),'chat_id':str(message.chat_id),
               'thread_id':str(getattr(message,'message_thread_id',None) or ''),'message_id':str(message.message_id)}
        authorized=self.adapter._is_callback_user_authorized(scope['owner_id'],chat_id=message.chat_id,
            chat_type=getattr(chat,'type',None),thread_id=scope['thread_id'] or None,user_name=getattr(user,'first_name',None))
        if not authorized:
            await query.answer('You are not authorized to inspect this job.');return
        # Stop Telegram's spinner before DB/job I/O, independently of a worker.
        if self._read is not None and not self._read.done():
            await query.answer('A native status read is still pending. Tap again after it completes.');return
        await query.answer()
        self._read=asyncio.create_task(asyncio.to_thread(self._resolve,parts[2],scope,parts[1]=='p'))
        self._read.add_done_callback(lambda task: task.exception() if not task.cancelled() else None)
        try:
            job=await asyncio.wait_for(asyncio.shield(self._read),self.read_budget)
        except asyncio.TimeoutError:
            await query.answer('Native status read has not returned; no work was restarted.');return
        except Exception:
            await query.answer('Native status is unavailable; no work was restarted.');return
        if job is None:
            # A scoped control forwarded to another chat must never change the original card.
            await query.answer('This control does not match the current job, message or owner.');return
        await query.edit_message_text(render(job,parts[1]=='p'),reply_markup=keyboard(parts[2]))


def register(ctx):
    def wire(application, adapter):
        from telegram.ext import CallbackQueryHandler
        controller=Controller(adapter)
        application.add_handler(CallbackQueryHandler(controller.handle,pattern=r'^jobs:[dp]:[A-Za-z0-9_-]+$'))
    ctx.register_telegram_handler(wire)
