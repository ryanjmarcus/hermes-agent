"""The real delegation/async admission boundary must never silently run inline."""
import json
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import tools.async_delegation as ad
import tools.delegate_tool as dt
from gateway import session_context as sc


@pytest.fixture
def runtime(monkeypatch):
    monkeypatch.setattr(ad, "_records", {})
    monkeypatch.setattr(ad, "_executor", None)
    monkeypatch.setattr(ad, "_executor_max_workers", 0)
    monkeypatch.setattr(dt, "_get_max_async_children", lambda: 1)
    monkeypatch.setattr(dt, "_resolve_delegation_credentials", lambda *a, **kw: {
        "model": "test-model", "provider": None, "base_url": None,
        "api_key": None, "api_mode": None, "command": None, "args": None,
    })
    parent = SimpleNamespace(_delegate_depth=0, session_id="admission-parent",
        _interrupt_requested=False, _active_children=[], _active_children_lock=threading.Lock())
    children = []

    def build(**kw):
        child = SimpleNamespace(_subagent_id=f"child-{len(children)}", _delegate_role="leaf",
            close=Mock(), tool_progress_callback=Mock(), get_activity_summary=lambda: {})
        children.append(child)
        parent._active_children.append(child)
        return child

    build_spy = Mock(side_effect=build)
    monkeypatch.setattr(dt, "_build_child_agent", build_spy)
    forbidden_run = Mock(side_effect=AssertionError("Rejected child must never execute"))
    monkeypatch.setattr(dt, "_run_single_child", forbidden_run)
    sc.set_session_vars(platform="telegram", chat_id="fixture-chat", session_id="admission-parent",
        session_key="admission-parent", async_delivery=True)
    yield parent, children, build_spy, forbidden_run
    if ad._executor is not None:
        ad._executor.shutdown(wait=True)
    for var in sc._VAR_MAP.values():
        var.set(sc._UNSET)
    sc._SESSION_ASYNC_DELIVERY.set(sc._UNSET)


def test_unsupported_delivery_rejects_before_constructing_children(runtime):
    parent, children, build, run = runtime
    sc.set_session_vars(platform="api_server", chat_id="", session_id="", session_key="",
        async_delivery=False)
    result = json.loads(dt.delegate_task(goal="fixture work", background=True, parent_agent=parent))
    assert result["status"] == "rejected"
    assert result["reason"] == "async_delivery_unsupported"
    assert result["started"] is False and result["queued"] is False
    assert result["retryable"] is False
    build.assert_not_called()
    run.assert_not_called()
    assert not children and not parent._active_children and not ad._records


def test_unknown_delivery_capability_rejects_closed(runtime, monkeypatch):
    parent, _, build, run = runtime
    def unavailable():
        raise RuntimeError("capability unavailable")
    monkeypatch.setattr(sc, "async_delivery_supported", unavailable)
    result = json.loads(dt.delegate_task(goal="fixture work", background=True, parent_agent=parent))
    assert result["status"] == "rejected"
    assert result["reason"] == "delivery_capability_unavailable"
    build.assert_not_called()
    run.assert_not_called()


def test_real_pool_exhaustion_returns_without_running_or_queuing_rejected_work(runtime):
    parent, children, _, run = runtime
    unrelated_child = object()
    parent._active_children.append(unrelated_child)
    entered, release = threading.Event(), threading.Event()
    def occupied():
        entered.set()
        assert release.wait(10), "test must release the accepted worker"
        return {"results": [{"status": "completed"}]}
    first = ad.dispatch_async_delegation_batch(goals=["occupy fixture slot"], context=None,
        toolsets=None, role="leaf", model="test-model", session_key="admission-parent",
        runner=occupied, max_async_children=1)
    try:
        assert first["status"] == "dispatched" and entered.wait(5)
        result = json.loads(dt.delegate_task(goal="second fixture work", background=True, parent_agent=parent))
        assert not release.is_set()  # the accepted worker is STILL occupied
        assert result["status"] == "rejected"
        assert "capacity" in result["error"].lower()
        assert result["started"] is False and result["queued"] is False
        assert result["retryable"] is True
        run.assert_not_called()
        assert len(children) == 1
        children[0].close.assert_called_once()
        assert parent._active_children == [unrelated_child]
        assert list(ad._records) == [first["delegation_id"]]
    finally:
        release.set()


def test_real_submit_failure_rejects_and_closes_unstarted_children(runtime, monkeypatch):
    parent, children, _, run = runtime
    stop_hook = Mock()
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", stop_hook)
    class UnavailableExecutor:
        def submit(self, *args, **kwargs):
            raise RuntimeError("fixture executor stopped")
    monkeypatch.setattr(ad, "_get_executor", lambda _: UnavailableExecutor())
    result = json.loads(dt.delegate_task(goal="fixture work", background=True, parent_agent=parent))
    assert result["status"] == "rejected"
    assert "Failed to schedule" in result["error"]
    assert result["started"] is False and result["queued"] is False
    run.assert_not_called()
    children[0].close.assert_called_once()
    assert not parent._active_children and not ad._records
    stop_hook.assert_called_once()
    assert stop_hook.call_args.args == ("subagent_stop",)
    assert stop_hook.call_args.kwargs["child_status"] == "rejected"


@pytest.mark.parametrize("entry", ["model", "registry"])
@pytest.mark.parametrize("threaded", [False, True])
def test_finite_cli_rejects_model_delegation_in_execution_context(runtime, monkeypatch, entry, threaded):
    from cli import _run_cli_agent_turn
    from run_agent import AIAgent
    from tools.registry import registry

    parent, _, build, run = runtime
    sc.reset_session_vars()
    monkeypatch.setenv("HERMES_SESSION_ID", "finite-cli-has-id-but-no-delivery-owner")
    monkeypatch.delenv("HERMES_SESSION_PLATFORM", raising=False)
    monkeypatch.delenv("HERMES_SESSION_CHAT_ID", raising=False)
    sc._SESSION_ASYNC_DELIVERY.set(sc._UNSET)
    engaged_before = sc._session_context_engaged
    observed = {}
    args = {"goal": "fixture work", "background": False, "max_iterations": 1}

    def model_turn(**kwargs):
        assert sc.async_delivery_supported() is False
        if entry == "model":
            return AIAgent._dispatch_delegate_task(parent, args)
        return registry.dispatch("delegate_task", args, parent_agent=parent)

    cli = SimpleNamespace(_single_query_mode=True, agent=SimpleNamespace(run_conversation=model_turn))
    def turn():
        try:
            observed["result"] = json.loads(_run_cli_agent_turn(cli, user_message="fixture"))
            observed["delivery_after"] = sc.async_delivery_supported()
        except BaseException as exc:
            observed["exception"] = exc

    if threaded:
        worker = threading.Thread(target=turn, daemon=True)
        worker.start()
        worker.join(5)
        assert not worker.is_alive(), "finite admission must return without waiting for a child"
    else:
        turn()
    assert "exception" not in observed, observed.get("exception")
    result = observed["result"]
    assert result["status"] == "rejected"
    assert result["reason"] == "async_delivery_unsupported"
    assert result["started"] is False and result["queued"] is False
    assert observed["delivery_after"] is True
    assert sc.async_delivery_supported() is True
    assert sc._session_context_engaged is engaged_before
    build.assert_not_called()
    run.assert_not_called()
    assert not ad._records


@pytest.mark.parametrize("owner", ["interactive", "telegram", "api_server"])
@pytest.mark.parametrize("entry", ["model", "registry"])
def test_model_dispatch_with_surviving_owner_still_admits(runtime, monkeypatch, owner, entry):
    from cli import _run_cli_agent_turn
    from run_agent import AIAgent
    from tools.registry import registry

    parent, _, build, _ = runtime
    entered, release = threading.Event(), threading.Event()
    def child_run(*args, **kwargs):
        entered.set()
        assert release.wait(10)
        return {"task_index": 0, "status": "completed", "summary": "fixture completed",
                "api_calls": 0, "duration_seconds": 0, "model": "test-model"}
    monkeypatch.setattr(dt, "_run_single_child", child_run)
    sc.set_session_vars(platform="api_server" if owner == "api_server" else "telegram",
        chat_id="surviving-owner", session_id="admission-parent", session_key="admission-parent",
        async_delivery=owner != "api_server")
    args = {"goal": "fixture work", "background": False}
    def model_turn(**kwargs):
        if entry == "model":
            return AIAgent._dispatch_delegate_task(parent, args)
        return registry.dispatch("delegate_task", args, parent_agent=parent)
    try:
        if owner == "interactive":
            cli = SimpleNamespace(_single_query_mode=False, agent=SimpleNamespace(run_conversation=model_turn))
            result = json.loads(_run_cli_agent_turn(cli, user_message="fixture"))
        else:
            result = json.loads(model_turn())
        assert result["status"] == "dispatched"
        assert entered.wait(5)
        assert not release.is_set()  # foreground returned while admitted work is still running
        build.assert_called_once()
        assert len(ad._records) == 1
    finally:
        release.set()
