# Native Telegram job cards and read-only controls

The standalone plugin observes the official `post_tool_call` hook and registers a scoped `jobs:` callback through `register_telegram_handler`. It uses Hermes `async_delegations` as the sole job ledger. Local SQLite contains only button bindings and message-delivery receipts, never worker execution state.

## Canary integration (not enabled in production)

1. Copy this directory into the isolated Hermes profile's `plugins/telegram-job-controls/` and enable it using the profile's native plugin configuration. It needs the candidate's declared python-telegram-bot22.8 dependency. Do not start a second bot poller or copy credentials.
2. No extra model tool or manual card-attachment call is required. An actual accepted `delegate_task` result (`status=dispatched`, `mode=background`, native delegation ID and goals) triggers the card. The hook snapshots only explicitly bound native ContextVars and rejects process-env fallback. It then matches the ledger's `origin_session`, rechecks adapter authorization, and schedules a supervised task on that profile's captured Telegram event loop.
3. The card includes the accepted goal, native job ID/state/timestamps and delivery information. The plugin attaches Details/Probe only after a successful native `SendResult` provides an actual message ID. It never guesses Telegram IDs or says work started before native admission. Duplicate observations use an atomic durable message claim to avoid posting duplicate cards.
4. Callbacks recheck owner, chat, topic, message, native delegation ID, `dispatched_at` and original session. Details retains the goal plus available native timestamps/result/delivery. Probe also samples the existing native in-memory progress API. Neither starts, cancels or retries work, and neither creates a model turn.
5. Status reads run off the event loop, with only one outstanding read per adapter. A two-second response budget reports pending inspection while retaining that read; this is an inspection response budget, not a job deadline or a diagnosis that the worker is hung. Scope must be verified before any card edit.
6. Native cold typing already starts while the message handler waits. For the foreground coordinator, `display.busy_input_mode: steer` and `display.busy_steer_ack_enabled: false` successfully steer compatible followups without interruption, queuing or a canned bubble. Native steer may fall back to queuing when the agent/input does not support steering. No automatic generic request acknowledgment is implemented.

## Validation

`tests/hermes_migration/` runs with actual Hermes imports, native SQLite job dispatch and actual Telegram SDK types. Only network methods are in-memory boundaries. Current result: **18 passed; four strict expected failures remain diagnostic native-adapter policy gaps**, not a claim that those gaps are repaired.

Coverage includes real accepted background dispatch -> hook -> captured Telegram loop -> goal card -> confirmed send ID -> button binding; duplicate observations, unbound/env-only context rejection, rejected dispatch rejection, revoked authorization, wrong owner/chat/topic/message, stale attempt, adapter reconstruction, repeated read-only clicks, unconfirmed send without blind retry, stalled probe admission, native handler pattern, useful details, actual cold typing and successful steer without canned messages.

## Explicit limits

- Ambiguous/cancelled card sends are recorded for inspection and are not blindly replayed. Already confirmed card controls survive restart. This is not a complete restart-replay outbox for cards.
- Do **not** put these mid-turn job cards in `gateway.delivery_ledger`: native recovery of final obligations clears session `resume_pending`, which would incorrectly treat a job card as the completed answer. Hermes currently provides no equivalent native nonfinal card delivery/rebind event. A future fix needs that exact native contract, not another general worker engine.
- Plugin enablement and a deliberate authorized bot cutover must still verify cold-start Telegram update retention. Core cold connect may drop pending updates.
- Automatic completion edits, Retry and Merge mutations are not implemented by this read-only change. Details/Probe show current native state on demand.
