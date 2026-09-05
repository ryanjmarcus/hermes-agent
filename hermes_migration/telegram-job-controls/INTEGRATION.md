# Read-only native Telegram job controls

The plugin persists only opaque callback bindings. Job state remains in Hermes `async_delegations`; the identity is `delegation_id + dispatched_at + origin_session`. It registers a pattern-scoped `jobs:` callback using the official plugin factory; existing approvals and choice pickers remain untouched.

## Integration points (not enabled in production)

1. Copy `telegram-job-controls/` into the **isolated Hermes profile's** `plugins/telegram-job-controls/` and enable it using the profile's native plugin configuration. It needs python-telegram-bot22.8, now installed in the canary venv by the integration agent. Do not copy credentials or start a second bot poller.
2. After the coordinator creates a real native background delegation and posts a meaningful job card, call the plugin module's `attach_controls(adapter, job_id=..., owner_id=..., chat_id=..., thread_id=..., message_id=..., origin_session=...)`. Arguments must come from the authenticated incoming source and the confirmed job/card, never the model's guessed chat/message IDs. `attach_controls` checks the native job's origin session before persisting buttons, then edits only the existing card's markup.
3. `Details` reads native persisted state. `Probe` also samples the existing native in-memory delegation registry/progress API. Neither operation starts, restarts, cancels or retries a worker, and neither creates a model turn. A job's stale `dispatched_at` or origin invalidates the old control.
4. Callbacks recheck adapter authorization plus exact owner/chat/topic/message identity. Binding storage survives adapter reconstruction. Tokens reveal no job/session identifiers and fit the 64-byte Telegram limit. Repeated read-only clicks are safe.
5. Status reads run off the event loop. One outstanding read per adapter prevents repeated taps from spawning abandoned reads. A two-second response budget returns a callback notice while preserving the outstanding read; this is an inspection response budget, **not a job lifetime or “hung” diagnosis**. No card text is changed until scope and native job identity are verified.

## Validation

`tests/hermes_migration/test_job_controls.py`: actual Hermes SQLite job ledger, TelegramAdapter and real python-telegram-bot SDK, with only Telegram network methods replaced by in-memory calls. Covers reconstructed adapter, repeat taps, wrong owner/chat/topic/message, changed attempt, revoked authorization, stalled probe bounded admission, native handler pattern and attaching only an existing same-session job.

`tests/hermes_migration/test_telegram_responsiveness_contract.py`: original native behavior gates plus actual cold typing scheduling. Four strict expected failures remain explicit unmet requirements of the original adapter; the plugin does not pretend to repair acknowledgment policy or the core ephemeral clarify buttons.

Native cold typing already starts immediately in `_process_message_background` and stays active while the handler waits. No typing hook or core patch is necessary. Meaningful acknowledgment still comes from coordinator behavior with actual job intent; no canned “received” response is added.

Remaining integration work: coordinator must call `attach_controls` using its verified dispatch/card receipts, plugin must be enabled in the canary profile, and a deliberate authorized bot cutover must verify cold-start update retention. Retry/Merge buttons are intentionally not implemented by this read-only patch.
