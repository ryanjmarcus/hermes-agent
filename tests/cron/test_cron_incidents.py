"""Durable cron failure incidents: signature dedup, lifecycle, ack, CLI."""

from __future__ import annotations

import argparse
import multiprocessing
import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import cron.incidents as incidents
import cron.jobs as cron_jobs
import cron.scheduler as sched


def _migrate_legacy_schema_worker(db_path: str, start, results) -> None:
    """Separate process: exercise migration without the module-local RLock."""
    incidents.EXECUTIONS_FILE = Path(db_path)
    start.wait(timeout=10)
    try:
        results.put(("ok", incidents.count_incidents()))
    except Exception as exc:  # pragma: no cover - asserted in parent
        results.put(("error", repr(exc)))


def _point_db(monkeypatch, tmp_path):
    """Point the incident store at a throwaway executions.db (same file shape
    the scheduler uses). ``cron.executions.EXECUTIONS_FILE`` stays None so the
    incident store falls back to its own override."""
    monkeypatch.setattr(incidents, "EXECUTIONS_FILE", tmp_path / "cron" / "executions.db")
    return incidents


def _job(**overrides):
    job = {
        "id": "incident-gating-test",
        "name": "incident gating test",
        "prompt": "hello",
        "enabled": True,
        "state": "scheduled",
        "schedule": {"kind": "interval", "minutes": 5, "display": "every 5m"},
        "deliver": "local",
        "model": None,
        "provider": None,
        "provider_snapshot": "openrouter",
        "base_url": None,
    }
    job.update(overrides)
    return job


def _tick_failing(job, tmp_path, deliveries, error="boom unrelated"):
    """Run one run_one_job tick whose agent raises ``error`` (the failure
    path that composes the per-run failure ping). Mirrors the drift-alert-once
    harness so the incident gating is exercised through the real scheduler."""
    fake_db = MagicMock()

    def fake_deliver(jb, content, adapters=None, loop=None):
        deliveries.append(content)
        return None

    with cron_jobs.use_cron_store(tmp_path), \
         patch("cron.scheduler._hermes_home", tmp_path), \
         patch("cron.scheduler._resolve_origin", return_value=None), \
         patch("hermes_cli.env_loader.load_hermes_dotenv"), \
         patch("hermes_cli.env_loader.reset_secret_source_cache"), \
         patch("hermes_state.SessionDB", return_value=fake_db), \
         patch("tools.mcp_tool.discover_mcp_tools", return_value=[]), \
         patch("hermes_cli.runtime_provider.resolve_runtime_provider",
               return_value={
                   "api_key": "test-key",
                   "base_url": "https://example.invalid/v1",
                   "provider": "openrouter",
                   "api_mode": "chat_completions",
               }), \
         patch.object(sched, "_deliver_result", side_effect=fake_deliver), \
         patch("run_agent.AIAgent") as mock_agent_cls:
        mock_agent = MagicMock()
        mock_agent.run_conversation.side_effect = RuntimeError(error)
        mock_agent_cls.return_value = mock_agent
        sched.run_one_job(dict(job))
    return mock_agent_cls.called


# ── Store + dedup ──────────────────────────────────────────────────────────


def test_new_failure_creates_incident_and_is_new(monkeypatch, tmp_path):
    inc = _point_db(monkeypatch, tmp_path)

    inc_id, is_new = inc.upsert_incident("job-1", "Provider timeout: read timed out")

    assert is_new is True
    assert inc_id.startswith("job-1_")
    row = inc.get_incident(inc_id)
    assert row is not None
    assert row["job_id"] == "job-1"
    assert row["state"] == "detected"
    assert row["failure_type"] == "timeout"
    assert row["first_seen_at"] == row["last_seen_at"]
    assert inc.count_incidents() == 1


def test_same_signature_dedups_same_incident(monkeypatch, tmp_path):
    inc = _point_db(monkeypatch, tmp_path)

    id1, new1 = inc.upsert_incident("job-1", "Provider timeout: read timed out")
    id2, new2 = inc.upsert_incident("job-1", "PROVIDER TIMEOUT: read timed out   ")

    assert id1 == id2, "normalized (case/whitespace) signature must dedup"
    assert new1 is True
    assert new2 is False
    assert inc.count_incidents() == 1
    # Refresh updates last_seen but never resets an open state.
    assert inc.get_incident(id1)["state"] == "detected"


def test_error_change_mints_new_incident(monkeypatch, tmp_path):
    inc = _point_db(monkeypatch, tmp_path)

    id1, _ = inc.upsert_incident("job-1", "provider timeout")
    id2, new2 = inc.upsert_incident("job-1", "provider rate limit 429")

    assert id1 != id2
    assert new2 is True
    assert inc.count_incidents() == 2


# ── Redaction / classification ─────────────────────────────────────────────


def test_redaction_applied_to_incident_error(monkeypatch, tmp_path):
    # agent.redact snapshots _REDACT_ENABLED from HERMES_REDACT_SECRETS at
    # module-import time. When another collected test module imports the
    # gateway/scheduler chain (e.g. test_codex_execution_paths.py), that
    # import happens at COLLECTION time — before the conftest env scrub —
    # so a developer shell exporting HERMES_REDACT_SECRETS=false freezes
    # redaction off and this test fails only in full-directory runs.
    # Pin the flag explicitly, matching the repo-wide pattern.
    monkeypatch.setattr("agent.redact._REDACT_ENABLED", True, raising=False)
    inc = _point_db(monkeypatch, tmp_path)
    secret = "ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij"

    inc_id, _ = inc.upsert_incident("job-1", f"failed: {secret} boom")

    row = inc.get_incident(inc_id)
    assert secret not in row["error"]
    assert "boom" in row["error"]


def test_error_truncated_to_bounded_length(monkeypatch, tmp_path):
    inc = _point_db(monkeypatch, tmp_path)
    long_error = "x" * 2000

    inc_id, _ = inc.upsert_incident("job-1", long_error)

    assert len(inc.get_incident(inc_id)["error"]) <= 500


def test_failure_type_classification(monkeypatch, tmp_path):
    inc = _point_db(monkeypatch, tmp_path)
    cases = [
        ("delivery failed for telegram chat", "delivery"),
        ("Provider read timed out after 60s", "timeout"),
        ("authentication failed: invalid API key", "auth"),
        ("HTTP 429: rate limit exceeded", "rate_limit"),
        ("configuration validation blocked the run", "config"),
        ("script exited with code 1", "script"),
        ("agent crashed mid-conversation", "agent"),
        ("something completely unexpected happened", "unknown"),
    ]
    for error, expected in cases:
        assert inc._classify_failure_type(error) == expected, (error, expected)


# ── Lifecycle / ack ────────────────────────────────────────────────────────


def test_lifecycle_transitions(monkeypatch, tmp_path):
    inc = _point_db(monkeypatch, tmp_path)
    inc_id, _ = inc.upsert_incident("job-1", "boom")

    assert inc.get_incident(inc_id)["state"] == "detected"
    assert inc.set_incident_state(inc_id, "alerted") is True
    assert inc.set_incident_state(inc_id, "closed") is True
    row = inc.get_incident(inc_id)
    assert row["state"] == "closed"
    assert row["acked_at"] and row["closed_at"]

    # Closed is terminal for that signature: no re-open, no re-transition.
    assert inc.set_incident_state(inc_id, "alerted") is False
    assert inc.set_incident_state(inc_id, "closed") is False
    assert inc.ack_incident(inc_id) is False
    assert inc.get_incident(inc_id)["state"] == "closed"

    # Invalid states are rejected, not raised.
    assert inc.set_incident_state(inc_id, "bogus") is False
    assert inc.list_incidents(state="bogus") == []
    assert inc.count_incidents(state="bogus") == 0


def test_acked_signature_stays_closed_on_refresh(monkeypatch, tmp_path):
    """Ack is per-signature: upserting the same error after ack must NOT
    resurrect the incident — a changed error is what mints a new one."""
    inc = _point_db(monkeypatch, tmp_path)
    inc_id, _ = inc.upsert_incident("job-1", "same failure text")
    inc.ack_incident(inc_id)

    same_id, is_new = inc.upsert_incident("job-1", "SAME FAILURE TEXT")

    assert same_id == inc_id
    assert is_new is False
    assert inc.get_incident(inc_id)["state"] == "closed"


def test_success_resolves_active_incidents_and_recurrence_reopens(monkeypatch, tmp_path):
    inc = _point_db(monkeypatch, tmp_path)
    first_id, _ = inc.upsert_incident("job-1", "same failure text")
    inc.set_incident_state(first_id, "alerted")
    closed_id, _ = inc.upsert_incident("job-1", "operator acked failure")
    inc.ack_incident(closed_id)
    other_id, _ = inc.upsert_incident("job-2", "other failure")

    assert inc.resolve_job_incidents("job-1") == 1
    resolved = inc.get_incident(first_id)
    closed = inc.get_incident(closed_id)
    other = inc.get_incident(other_id)
    assert resolved is not None
    assert closed is not None
    assert other is not None
    assert resolved["state"] == "resolved"
    assert resolved["resolved_at"]
    assert closed["state"] == "closed"
    assert other["state"] == "detected"

    same_id, is_new = inc.upsert_incident("job-1", "SAME FAILURE TEXT")
    assert same_id == first_id
    assert is_new is True
    reopened = inc.get_incident(first_id)
    assert reopened is not None
    assert reopened["state"] == "detected"
    assert reopened["resolved_at"] is None


def test_scheduler_success_resolution_is_best_effort():
    with patch("cron.incidents.resolve_job_incidents") as resolve:
        sched._resolve_incidents_after_success("job-1")
    resolve.assert_called_once_with("job-1")

    with patch("cron.incidents.resolve_job_incidents", side_effect=RuntimeError("db locked")):
        sched._resolve_incidents_after_success("job-1")


# ── Missing DB / lazy schema ───────────────────────────────────────────────


def test_missing_db_no_crash(monkeypatch, tmp_path):
    inc = _point_db(monkeypatch, tmp_path)

    inc_id, is_new = inc.upsert_incident("job-1", "boom")

    assert is_new is True
    assert (tmp_path / "cron" / "executions.db").is_file()
    assert inc.list_incidents() == [inc.get_incident(inc_id)]
    assert inc.count_incidents() == 1
    assert inc.get_incident("nope") is None


def test_legacy_schema_migration_is_safe_across_processes(monkeypatch, tmp_path):
    db_path = tmp_path / "cron" / "executions.db"
    db_path.parent.mkdir(parents=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """CREATE TABLE cron_incidents (
                 id TEXT PRIMARY KEY, job_id TEXT NOT NULL, error_sig TEXT NOT NULL,
                 state TEXT NOT NULL, failure_type TEXT NOT NULL DEFAULT 'unknown',
                 first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
                 acked_at TEXT, closed_at TEXT, error TEXT NOT NULL, output_file TEXT
               )"""
        )

    ctx = multiprocessing.get_context("spawn")
    start = ctx.Event()
    results = ctx.Queue()
    workers = [
        ctx.Process(target=_migrate_legacy_schema_worker, args=(str(db_path), start, results))
        for _ in range(4)
    ]
    for worker in workers:
        worker.start()
    start.set()
    outcomes = [results.get(timeout=15) for _ in workers]
    for worker in workers:
        worker.join(timeout=15)
        assert worker.exitcode == 0

    assert outcomes == [("ok", 0)] * 4
    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(cron_incidents)")}
    assert "resolved_at" in columns


# ── Scheduler gating ───────────────────────────────────────────────────────


def test_unacked_failure_still_alerts(monkeypatch, tmp_path):
    inc = _point_db(monkeypatch, tmp_path)
    deliveries = []
    job = _job()
    with cron_jobs.use_cron_store(tmp_path):
        cron_jobs.save_jobs([job])
        _tick_failing(job, tmp_path, deliveries, error="unacked boom")
        _tick_failing(job, tmp_path, deliveries, error="unacked boom")

    assert len(deliveries) == 2, "unacked failures must keep alerting per run"
    rows = inc.list_incidents()
    assert len(rows) == 1
    assert rows[0]["state"] == "detected"


def test_ack_suppresses_alert_until_signature_changes(monkeypatch, tmp_path):
    inc = _point_db(monkeypatch, tmp_path)
    deliveries = []
    job = _job()
    with cron_jobs.use_cron_store(tmp_path):
        cron_jobs.save_jobs([job])
        # First failure: alert delivered, incident minted.
        _tick_failing(job, tmp_path, deliveries, error="boom signature A")
        assert len(deliveries) == 1
        rows = inc.list_incidents()
        assert len(rows) == 1 and rows[0]["state"] == "detected"
        inc_id = rows[0]["id"]

        # Acknowledge it.
        assert inc.ack_incident(inc_id) is True

        # Same signature: alert suppressed, incident stays closed.
        _tick_failing(job, tmp_path, deliveries, error="boom signature A")
        assert len(deliveries) == 1, "acked signature must not re-ping"
        assert inc.get_incident(inc_id)["state"] == "closed"

        # Changed signature: new incident, alert again.
        _tick_failing(job, tmp_path, deliveries, error="boom signature B")
        assert len(deliveries) == 2, "changed signature must re-alert"
        assert inc.count_incidents() == 2


def test_mark_incident_alerted_sets_state_never_resurrects(monkeypatch, tmp_path):
    """The post-delivery 'alerted' transition records that a ping went out,
    and is a no-op on a closed (acked) incident — it can never resurrect one."""
    inc = _point_db(monkeypatch, tmp_path)

    inc_id, _ = inc.upsert_incident("job-1", "boom")
    sched._mark_incident_alerted(inc_id)
    assert inc.get_incident(inc_id)["state"] == "alerted"

    inc.ack_incident(inc_id)
    sched._mark_incident_alerted(inc_id)
    assert inc.get_incident(inc_id)["state"] == "closed"

    # Best-effort: bad/missing ids never raise.
    sched._mark_incident_alerted(None)
    sched._mark_incident_alerted("nonexistent")


def test_best_effort_incident_store_failure_returns_false(monkeypatch, tmp_path):
    """An incident-store error must never break the cron delivery path."""
    _point_db(monkeypatch, tmp_path)
    with patch("cron.incidents.upsert_incident",
               side_effect=RuntimeError("db locked")):
        assert sched._upsert_incident_for_failure(_job(), "boom") == (False, None)


# ── CLI ────────────────────────────────────────────────────────────────────


def test_cli_list_and_ack(monkeypatch, tmp_path, capsys):
    from hermes_cli.cron import cron_incidents

    inc = _point_db(monkeypatch, tmp_path)
    inc_id, _ = inc.upsert_incident("job-1", "provider timeout boom")

    # List.
    list_args = argparse.Namespace(
        incident_action="list", state=None, incident_id=None
    )
    assert cron_incidents(list_args) == 0
    out = capsys.readouterr().out
    assert inc_id in out
    assert "job-1" in out

    # State filter.
    filter_args = argparse.Namespace(
        incident_action="list", state="closed", incident_id=None
    )
    assert cron_incidents(filter_args) == 0
    out = capsys.readouterr().out
    assert "No cron failure incidents recorded." in out

    # Ack.
    ack_args = argparse.Namespace(
        incident_action="ack", state=None, incident_id=inc_id
    )
    assert cron_incidents(ack_args) == 0
    assert inc.get_incident(inc_id)["state"] == "closed"
    out = capsys.readouterr().out
    assert "acknowledged" in out.lower()

    # Ack again: already closed, still a clean exit.
    assert cron_incidents(ack_args) == 0
    out = capsys.readouterr().out
    assert "already closed" in out.lower()

    # Ack with a missing id is a usage error.
    missing_args = argparse.Namespace(
        incident_action="ack", state=None, incident_id=None
    )
    assert cron_incidents(missing_args) == 1
