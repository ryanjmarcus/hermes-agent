# Hermes Telegram migration contract

Pinned source: v0.21.0 `29112bef099274229cadff79cdff7bf7b99c4b77`.
Isolated Mini worktree: `/private/tmp/hermes-telegram-offline`, branch `codex/telegram-offline-contract`.
This harness makes no Telegram connection, sends no live messages, reads no credentials, and changes no production configuration. The actual Hermes adapter and runner are imported; only the Telegram transport boundary is mocked. The canonical test wrapper strips credentials and isolates HERMES_HOME.

## Existing implementation to reuse

- `tools/async_delegation.py`: persisted dispatch and terminal result in SQLite `state.db`; background dispatch returns a delegation ID. Completion events use the existing process-registry queue. Startup classifies dead owners as `unknown`, then replays undelivered terminal events. This does **not** resume interrupted work or provide admission retries.
- `gateway/delivery_ledger.py`: durable final-response obligations; pending/attempting/delivered states, bounded recovery, visible marker for ambiguous retransmission. It is best effort; ledger errors intentionally do not block sends. It is not a durable arbitrary job-card/action store.
- Native Kanban: durable SQLite tasks, task/run IDs, process claim/reclaim and dispatcher lifecycle hooks. Prefer this existing owner when work must remain pending across capacity rejection or restart; do not treat the completion queue as a work-admission queue.
- `plugins/platforms/telegram/adapter.py`: real forum topic metadata, typing, native approval/clarify keyboards and direct callback handling. Clarify/approval routing maps live on the adapter; their existence is not evidence of restart-safe buttons.
- `hermes_cli/plugins.py`, `PluginContext.register_telegram_handler(factory)`: invoked before core handlers. Register a pattern-scoped callback (`^jobs:`), not a competing catch-all. Separate pre-handler group can implement an authenticated receipt without editing Hermes core. Authorization and topic/group gating must be equivalent to the real inbound route, and receipt must say “received”, not claim dispatch before a job exists.

## Behavior measured offline

Canonical command:

```
HERMES_PYTHON=<candidate>/code/.venv/bin/python scripts/run_tests.sh tests/gateway/test_telegram_responsiveness_contract.py -j 1 --file-retries 0
```

Result: **2 passed, 4 strict expected failures**. The four expected failures are unfinished migration requirements, not successful fixes.

Passing:
- With input queue mode and the normal busy-text route, an active worker does not prevent immediate acknowledgment; topic 12 stays attached and the worker is not interrupted.
- Native “Retry/No” choice resolves directly to its registered choice, preserves the topic and rejects a repeated tap after resolution.

Unmet:
- Cold adapter dispatch starts processing without guaranteed application-level acknowledgment.
- A second distinct busy request within 30 seconds gets no receipt.
- Legacy `busy_text_mode=queue` returns before the busy-ack branch.
- Reconstructed adapter loses pending clarify routing; the prior button cannot resolve after restart.

Additional source constraint: Telegram `connect(is_reconnect=False)` documents dropping pending updates at cold boot; reconnect preserves them. Migration startup must explicitly verify retention behavior before switching the bot, not merely test a running connection.

## Smallest follow-on implementation

1. Use the native Telegram plugin handler factory for a receipt before the normal handler and for `jobs:` callbacks. Keep the normal core approval/model-picker/clarify handlers intact. Durable receipt/action ownership should reference existing Kanban task/run IDs, not a second broad job framework.
2. Persist callback action ID, task/run revision, actor authorization scope, chat/topic and transition outcome. Consume callbacks idempotently and acknowledge callback immediately. Status/probe reads must not enqueue behind a model worker; retry must inspect actual task/branch/artifact state first.
3. Reuse native final-delivery ledger for terminal narrative results. Wire job-card updates from Kanban lifecycle observers, with explicit recovery of outstanding card updates; do not assume the final-response ledger already covers editable cards.
4. Prove startup update retention, restart during callback processing, unauthorized/cross-topic taps and duplicate taps, plus receipt while a real bounded child process runs, before any production switch.

No runtime patch or broad framework is included in this bounded feasibility task.
