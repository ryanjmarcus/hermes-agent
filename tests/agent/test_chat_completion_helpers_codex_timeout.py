"""Regression tests for Codex SSE watchdog defaults."""

from agent.chat_completion_helpers import codex_event_stale_timeout_default


def test_small_codex_prompts_allow_transient_sse_event_gaps():
    # 12 seconds was too short for a stream that had already emitted its first
    # event; the configured environment override remains applied by the caller.
    assert codex_event_stale_timeout_default(0) == 30.0
    assert codex_event_stale_timeout_default(10_000) == 30.0


def test_large_codex_prompts_keep_scaled_event_stale_defaults():
    assert codex_event_stale_timeout_default(10_001) == 60.0
    assert codex_event_stale_timeout_default(50_001) == 120.0
    assert codex_event_stale_timeout_default(100_001) == 180.0
