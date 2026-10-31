from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import subprocess
import sys

import pytest
from workflow_recovery.cli import demo
from workflow_recovery.engine import ApprovalRequired, ConflictError, Engine

RUNNER = Path(__file__).resolve().parents[1] / "run.py"


def crash(directory, workflow_id, flag):
    process = subprocess.run([sys.executable, str(RUNNER), "worker", "--directory", str(directory), "--id", workflow_id, flag], timeout=30, capture_output=True)
    assert process.returncode in (23, 24), process.stderr.decode()


def test_hard_process_crash_between_tool_commit_and_checkpoint(tmp_path):
    Engine(tmp_path).create("order")
    crash(tmp_path, "order", "--crash-after-reservation")
    engine = Engine(tmp_path)
    before = engine.snapshot("order", "after crash")
    assert before["state"] == "CREATED"
    assert before["reservation_count"] == 1
    assert before["stock_remaining"] == 4
    engine.resume("order")
    recovered = Engine(tmp_path).snapshot("order", "recovered")
    assert recovered["state"] == "AWAITING_APPROVAL"
    assert recovered["reservation_count"] == 1
    assert recovered["stock_remaining"] == 4


def test_approval_and_dispatch_survive_restart_and_ack_gap(tmp_path):
    engine = Engine(tmp_path)
    engine.create("order")
    with pytest.raises(ApprovalRequired):
        engine.approve("order", "reviewer")
    engine.resume("order")
    with pytest.raises(ApprovalRequired):
        engine.dispatch("order")
    assert engine.resume("order") == "AWAITING_APPROVAL"
    engine.approve("order", "reviewer")
    crash(tmp_path, "order", "--crash-after-dispatch")
    engine = Engine(tmp_path)
    assert engine.snapshot("order", "crash")["state"] == "APPROVED"
    assert engine.snapshot("order", "crash")["dispatch_count"] == 1
    engine.dispatch("order")
    engine.dispatch("order")
    assert engine.snapshot("order", "done")["dispatch_count"] == 1
    assert engine.snapshot("order", "done")["state"] == "DISPATCHED"


def test_concurrent_workers_only_reserve_and_dispatch_once(tmp_path):
    Engine(tmp_path).create("order")
    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(lambda _: Engine(tmp_path).resume("order"), range(12)))
    engine = Engine(tmp_path)
    engine.approve("order", "reviewer")
    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(lambda _: Engine(tmp_path).dispatch("order"), range(12)))
    state = engine.snapshot("order", "done")
    assert state["reservation_count"] == state["dispatch_count"] == 1
    assert state["stock_remaining"] == 4
    assert len(state["events"]) == 4


def test_reused_keys_and_invalid_approval_are_rejected(tmp_path):
    engine = Engine(tmp_path)
    engine.create("order")
    engine.create("order")
    with pytest.raises(ConflictError):
        engine.create("order", "other-sku")
    engine.resume("order")
    with pytest.raises(ValueError):
        engine.approve("order", " ")
    engine.approve("order", "reviewer")
    with pytest.raises(ConflictError):
        engine.approve("order", "someone-else")


def test_out_of_stock_rolls_back_and_does_not_acknowledge(tmp_path):
    engine = Engine(tmp_path)
    for index in range(5):
        engine.create(str(index))
        engine.resume(str(index))
    engine.create("sixth")
    with pytest.raises(ValueError, match="stock"):
        engine.resume("sixth")
    state = engine.snapshot("sixth", "no stock")
    assert state["state"] == "CREATED"
    assert state["reservation_count"] == 0
    assert state["stock_remaining"] == 0


def test_demo_exports_real_crash_and_recovery_checks(tmp_path):
    assert demo(tmp_path) == 0
    report = json.loads((tmp_path / "demo.json").read_text())
    assert report["checks"]["real_worker_exit_code"] == 23
    assert report["checks"]["approval_enforced"] is True
    assert len(report["stages"]) == 7
    assert report["stages"][-1]["dispatch_count"] == 1
