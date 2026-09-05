"""Native Telegram job inspection; no worker execution, generic receipts or polling."""

import asyncio
import contextvars
import hashlib
import logging
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

            path = get_hermes_home() / "plugins/telegram-job-controls/buttons.db"
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with self.connect() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS buttons (token TEXT PRIMARY KEY, binding TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS cards (card_key TEXT PRIMARY KEY, state TEXT NOT NULL, receipt TEXT NOT NULL)"
            )
        self.path.chmod(0o600)

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.path, timeout=1)
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def issue(self, *, job, owner_id, chat_id, thread_id, message_id):
        for field in ("delegation_id", "dispatched_at", "origin_session"):
            if not job.get(field):
                raise ValueError("Job must exist before issuing controls: " + field)
        token = secrets.token_urlsafe(16)
        binding = {
            "job_id": job["delegation_id"],
            "attempt": job["dispatched_at"],
            "origin_session": job["origin_session"],
            "owner_id": str(owner_id),
            "chat_id": str(chat_id),
            "thread_id": str(thread_id or ""),
            "message_id": str(message_id),
            "title": str(job.get("title") or "")[:600],
            "created_at": time.time(),
        }
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO buttons VALUES (?,?)", (token, json.dumps(binding))
            )
        return token

    def get(self, token):
        with self.connect() as conn:
            row = conn.execute(
                "SELECT binding FROM buttons WHERE token=?", (token,)
            ).fetchone()
        return json.loads(row[0]) if row else None

    def claim_card(self, job, source):
        identity = [
            job["delegation_id"],
            job["dispatched_at"],
            job["origin_session"],
            source,
        ]
        key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        with self.connect() as conn:
            cursor = conn.execute(
                "INSERT OR IGNORE INTO cards VALUES (?, ?, ?)",
                (
                    key,
                    "sending",
                    json.dumps({
                        "job_id": job["delegation_id"],
                        "attempt": job["dispatched_at"],
                        "source": source,
                    }),
                ),
            )
            return key if cursor.rowcount else None

    def record_card(self, key, state, **receipt):
        with self.connect() as conn:
            current = conn.execute(
                "SELECT receipt FROM cards WHERE card_key=?", (key,)
            ).fetchone()
            if current:
                value = {**json.loads(current[0]), **receipt}
                conn.execute(
                    "UPDATE cards SET state=?, receipt=? WHERE card_key=?",
                    (state, json.dumps(value), key),
                )

    def confirmed_cards(self):
        with self.connect() as conn:
            rows = conn.execute("SELECT card_key, receipt FROM cards").fetchall()
        return [
            (key, json.loads(raw))
            for key, raw in rows
            if json.loads(raw).get("message_id")
        ]

    def claim_terminal_edit(self, key, job):
        """Persist edit intent before network I/O; ambiguous edits are inspectable."""
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT receipt FROM cards WHERE card_key=?", (key,)
            ).fetchone()
            if not row:
                return False
            receipt = json.loads(row[0])
            if (
                receipt.get("terminal_state")
                or not receipt.get("message_id")
                or receipt["attempt"] != job["dispatched_at"]
                or receipt["source"]["session_key"] != job["origin_session"]
            ):
                return False
            receipt.update(
                terminal_state=job["state"],
                terminal_completed_at=job.get("completed_at"),
                terminal_edit="attempting",
            )
            conn.execute(
                "UPDATE cards SET receipt=? WHERE card_key=?",
                (json.dumps(receipt), key),
            )
            return True


def native_job(job_id, probe=False):
    from tools.async_delegation import get_durable_delegation, list_async_delegations

    job = get_durable_delegation(job_id)
    if job and probe:
        current = next(
            (
                row
                for row in list_async_delegations()
                if row.get("delegation_id") == job_id
            ),
            None,
        )
        job["live_registry_present"] = current is not None
        if current:
            for key in ("seconds_since_progress", "children_activity", "in_tool"):
                if key in current:
                    job[key] = current[key]
    return job


def keyboard(token):
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Details", callback_data="jobs:d:" + token),
            InlineKeyboardButton("Probe", callback_data="jobs:p:" + token),
        ]
    ])


async def attach_controls(
    adapter,
    *,
    job_id,
    owner_id,
    chat_id,
    thread_id,
    message_id,
    origin_session,
    bindings=None,
    title="",
):
    """Called by the coordinator only after a real job and its card exist."""
    job = await asyncio.to_thread(native_job, job_id)
    if not job or job.get("origin_session") != origin_session:
        raise ValueError("Job does not belong to the coordinator session")
    job["title"] = title
    store = bindings or Bindings()
    token = await asyncio.to_thread(
        store.issue,
        job=job,
        owner_id=owner_id,
        chat_id=chat_id,
        thread_id=thread_id,
        message_id=message_id,
    )
    await adapter._bot.edit_message_reply_markup(
        chat_id=int(chat_id), message_id=int(message_id), reply_markup=keyboard(token)
    )
    return {
        "job_id": job_id,
        "attempt": job["dispatched_at"],
        "message_id": str(message_id),
        "token": token,
    }


def render(job, probe=False):
    from datetime import datetime, timezone

    lines = []
    if job.get("title"):
        lines.append(str(job["title"])[:600])
    lines.extend([f"Job {job['delegation_id']}", f"State: {job['state']}"])
    for key, label in [("dispatched_at", "Started"), ("completed_at", "Completed")]:
        stamp = job.get(key)
        if isinstance(stamp, (float, int)):
            lines.append(
                f"{label}: {datetime.fromtimestamp(stamp, timezone.utc).isoformat(timespec='seconds')}"
            )
    lines.append(
        f"Delivery: {job.get('delivery_state', 'unknown')} (attempts: {job.get('delivery_attempts', 0)})"
    )
    result = job.get("result") or {}
    if isinstance(result, dict):
        summary = result.get("summary") or result.get("error")
        if not summary and isinstance(result.get("results"), list):
            summary = "; ".join(
                str(row.get("summary") or row.get("error") or row.get("status") or "")
                for row in result["results"]
                if isinstance(row, dict)
            )
        if summary:
            lines.append("Result: " + str(summary)[:1200])
    if probe:
        lines.append(
            "Live registry: "
            + ("present" if job.get("live_registry_present") else "not present")
        )
        if "seconds_since_progress" in job:
            lines.append(f"Seconds since progress: {job['seconds_since_progress']}")
        if "in_tool" in job:
            lines.append(f"Tool active: {bool(job['in_tool'])}")
        for child in (job.get("children_activity") or [])[:5]:
            if isinstance(child, dict):
                lines.append(
                    f"Child: calls={child.get('api_calls', '?')}, tool={child.get('current_tool') or 'none'}, idle_seconds={child.get('seconds_since_activity', '?')}"
                )
        lines.append("Probe does not restart or duplicate work.")
        if job["state"] == "unknown":
            lines.append(
                "The prior owner exited without a confirmed result; inspect artifacts before retrying."
            )
    return "\n".join(lines)[:3800]


class Controller:
    def __init__(self, adapter, bindings=None, read_job=native_job, read_budget=2):
        self.adapter = adapter
        self.bindings = bindings or Bindings()
        self.read_job = read_job
        self.read_budget = read_budget
        self._read = None

    def _resolve(self, token, scope, probe):
        binding = self.bindings.get(token)
        if not binding or any(binding.get(k) != v for k, v in scope.items()):
            return None
        job = self.read_job(binding["job_id"], probe=probe)
        if (
            not job
            or job.get("dispatched_at") != binding["attempt"]
            or job.get("origin_session") != binding["origin_session"]
        ):
            return None
        job["title"] = binding.get("title", "")
        return job

    async def handle(self, update, context):
        query = update.callback_query
        data = getattr(query, "data", "") or ""
        parts = data.split(":")
        if len(parts) != 3 or parts[0] != "jobs" or parts[1] not in ("d", "p"):
            return
        message = getattr(query, "message", None)
        user = getattr(query, "from_user", None)
        if not message or not user:
            await query.answer("This job control is unavailable.")
            return
        chat = getattr(message, "chat", None)
        scope = {
            "owner_id": str(user.id),
            "chat_id": str(message.chat_id),
            "thread_id": str(getattr(message, "message_thread_id", None) or ""),
            "message_id": str(message.message_id),
        }
        authorized = self.adapter._is_callback_user_authorized(
            scope["owner_id"],
            chat_id=message.chat_id,
            chat_type=getattr(chat, "type", None),
            thread_id=scope["thread_id"] or None,
            user_name=getattr(user, "first_name", None),
        )
        if not authorized:
            await query.answer("You are not authorized to inspect this job.")
            return
        # Stop Telegram's spinner before DB/job I/O, independently of a worker.
        if self._read is not None and not self._read.done():
            await query.answer(
                "A native status read is still pending. Tap again after it completes."
            )
            return
        await query.answer()
        self._read = asyncio.create_task(
            asyncio.to_thread(self._resolve, parts[2], scope, parts[1] == "p")
        )
        self._read.add_done_callback(
            lambda task: task.exception() if not task.cancelled() else None
        )
        try:
            job = await asyncio.wait_for(asyncio.shield(self._read), self.read_budget)
        except asyncio.TimeoutError:
            await query.answer(
                "Native status read has not returned; no work was restarted."
            )
            return
        except Exception:
            await query.answer("Native status is unavailable; no work was restarted.")
            return
        if job is None:
            # A scoped control forwarded to another chat must never change the original card.
            await query.answer(
                "This control does not match the current job, message or owner."
            )
            return
        await query.edit_message_text(
            render(job, parts[1] == "p"), reply_markup=keyboard(parts[2])
        )


class AutomaticCards:
    """Observe accepted native dispatches; post one card, without running work."""

    def __init__(self, ctx):
        self.ctx = ctx
        self.adapters = {}

    def connected(self, adapter):
        owner = getattr(adapter, "_owner_profile", None) or "default"
        self.adapters[owner] = (adapter, asyncio.get_running_loop())
        self.ctx.spawn_task(self.reconcile(adapter), name="telegram-job-card-reconnect")

    def completed(self, event, **_):
        if not isinstance(event, dict) or not event.get("delegation_id"):
            return
        # Destinations come exclusively from durable confirmed card receipts.
        for adapter, loop in tuple(self.adapters.values()):
            if not loop.is_closed():
                loop.call_soon_threadsafe(
                    lambda adapter=adapter: self.ctx.spawn_task(
                        self.reconcile(adapter, event),
                        name="telegram-job-card-completion",
                    )
                )

    async def reconcile(self, adapter, event=None):
        store = Bindings()
        for key, receipt in await asyncio.to_thread(store.confirmed_cards):
            source = receipt["source"]
            if (source.get("profile") or "default") != (
                getattr(adapter, "_owner_profile", None) or "default"
            ):
                continue
            if event and (
                event.get("delegation_id") != receipt["job_id"]
                or event.get("dispatched_at") != receipt["attempt"]
                or event.get("session_key") != source["session_key"]
            ):
                continue
            job = await asyncio.to_thread(native_job, receipt["job_id"])
            if (
                not job
                or job.get("dispatched_at") != receipt["attempt"]
                or job.get("origin_session") != source["session_key"]
                or job.get("state") in ("running", "dispatched", "stalling")
            ):
                continue
            if event and event.get("status") != job["state"]:
                continue
            if not adapter._is_callback_user_authorized(
                source["user_id"],
                chat_id=source["chat_id"],
                chat_type=source["chat_type"],
                thread_id=source["thread_id"] or None,
            ):
                continue
            if not await asyncio.to_thread(store.claim_terminal_edit, key, job):
                continue
            job["title"] = receipt.get("title", "")
            try:
                kwargs = (
                    {"reply_markup": keyboard(receipt["token"])}
                    if receipt.get("token")
                    else {}
                )
                await adapter._bot.edit_message_text(
                    chat_id=int(source["chat_id"]),
                    message_id=int(receipt["message_id"]),
                    text=render(job),
                    **kwargs,
                )
                await asyncio.to_thread(
                    store.record_card,
                    key,
                    "terminal_updated",
                    terminal_edit="confirmed",
                )
            except Exception as error:
                await asyncio.to_thread(
                    store.record_card,
                    key,
                    "inspection_required",
                    terminal_edit="unconfirmed",
                    error_type=type(error).__name__,
                )
                logging.getLogger(__name__).warning(
                    "Job card edit unconfirmed (%s)", type(error).__name__
                )

    def post_tool(self, tool_name, result, **_):
        if tool_name != "delegate_task":
            return
        try:
            accepted = json.loads(result) if isinstance(result, str) else result
        except (TypeError, ValueError):
            return
        if (
            not isinstance(accepted, dict)
            or accepted.get("status") != "dispatched"
            or accepted.get("mode") != "background"
        ):
            return
        if not accepted.get("delegation_id"):
            return
        # Read only values actually bound in ContextVars. Never use the legacy
        # process-environment fallback or model-supplied destination arguments.
        context = {var.name: value for var, value in contextvars.copy_context().items()}
        names = (
            "PLATFORM",
            "CHAT_ID",
            "CHAT_TYPE",
            "THREAD_ID",
            "USER_ID",
            "SESSION_KEY",
            "PROFILE",
        )
        source = {
            name.lower(): context.get("HERMES_SESSION_" + name, "") for name in names
        }
        # SESSION_KEY's public ContextVar name is HERMES_SESSION_KEY.
        source["session_key"] = context.get("HERMES_SESSION_KEY", "")
        if source["platform"] != "telegram" or not all(
            isinstance(source[key], str) and source[key]
            for key in ("chat_id", "user_id", "session_key")
        ):
            return
        target = self.adapters.get(source["profile"] or "default")
        if target is None:
            return
        adapter, loop = target
        if loop.is_closed():
            return

        def schedule():
            self.ctx.spawn_task(
                self.publish(adapter, source, accepted), name="telegram-native-job-card"
            )

        loop.call_soon_threadsafe(schedule)

    async def publish(self, adapter, source, accepted):
        job = await asyncio.to_thread(native_job, accepted["delegation_id"])
        if not job or job.get("origin_session") != source["session_key"]:
            return
        if not adapter._is_callback_user_authorized(
            source["user_id"],
            chat_id=source["chat_id"],
            chat_type=source["chat_type"],
            thread_id=source["thread_id"] or None,
        ):
            return
        goals = accepted.get("goals") or []
        title = "; ".join(str(goal) for goal in goals if isinstance(goal, str))[:600]
        if not title:
            return
        store = Bindings()
        key = await asyncio.to_thread(store.claim_card, job, source)
        if key is None:
            return
        try:
            job["title"] = title
            sent = await adapter.send(
                source["chat_id"],
                render(job),
                metadata={"thread_id": source["thread_id"]}
                if source["thread_id"]
                else {},
            )
            if not sent.success or not sent.message_id:
                await asyncio.to_thread(store.record_card, key, "delivery_unconfirmed")
                return
            await asyncio.to_thread(
                store.record_card,
                key,
                "sent",
                message_id=str(sent.message_id),
                title=title,
            )
            controls = await attach_controls(
                adapter,
                job_id=job["delegation_id"],
                owner_id=source["user_id"],
                chat_id=source["chat_id"],
                thread_id=source["thread_id"],
                message_id=sent.message_id,
                origin_session=source["session_key"],
                bindings=store,
                title=title,
            )
            await asyncio.to_thread(
                store.record_card, key, "controls_attached", token=controls["token"]
            )
            # A very fast worker may have completed before its send receipt existed.
            await self.reconcile(adapter)
        except Exception as error:
            await asyncio.to_thread(
                store.record_card,
                key,
                "inspection_required",
                error_type=type(error).__name__,
            )
            logging.getLogger(__name__).warning(
                "Native job card needs inspection (%s)", type(error).__name__
            )


def register(ctx):
    cards = AutomaticCards(ctx)
    ctx.register_hook("post_tool_call", cards.post_tool)
    ctx.register_hook("on_async_delegation_completed", cards.completed)

    def wire(application, adapter):
        from telegram.ext import CallbackQueryHandler

        cards.connected(adapter)
        controller = Controller(adapter)
        application.add_handler(
            CallbackQueryHandler(
                controller.handle, pattern=r"^jobs:[dp]:[A-Za-z0-9_-]+$"
            )
        )

    ctx.register_telegram_handler(wire)
