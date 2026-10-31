import argparse
import json
from pathlib import Path
import subprocess
import sys
import uuid

from .engine import ApprovalRequired, Engine

LIMITATIONS = [
    "Deterministic local workflow; no language model plans or executes actions.",
    "Inventory and dispatch are sandbox database side effects, not real shipments.",
    "Approval records an actor string; authentication and remote authorization are not implemented.",
    "SQLite write serialization is local, not a distributed lease or exactly-once network guarantee.",
    "Snapshots are captured with workers quiescent; cross-database reads are not an atomic distributed snapshot.",
]


def demo(output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex
    directory = output / "runs" / run_id
    engine = Engine(directory)
    workflow_id = "shipment-1042"
    engine.create(workflow_id)
    stages = [engine.snapshot(workflow_id, "Requested")]
    runner = Path(__file__).resolve().parents[2] / "run.py"
    child = subprocess.run([sys.executable, str(runner), "worker", "--directory", str(directory), "--id", workflow_id, "--crash-after-reservation"], check=False, timeout=30)
    if child.returncode != 23:
        raise RuntimeError(f"Expected injected hard-exit23; got {child.returncode}")
    engine = Engine(directory)
    stages.append(engine.snapshot(workflow_id, "Worker crashed after tool commit"))
    engine.resume(workflow_id)
    stages.append(engine.snapshot(workflow_id, "Resumed without duplicate reservation"))
    denied = False
    try:
        engine.dispatch(workflow_id)
    except ApprovalRequired:
        denied = True
    stages.append(engine.snapshot(workflow_id, "Dispatch denied before approval"))
    engine.approve(workflow_id, "demo-reviewer")
    stages.append(engine.snapshot(workflow_id, "Approved"))
    Engine(directory).resume(workflow_id)
    stages.append(engine.snapshot(workflow_id, "Dispatched"))
    for _ in range(3):
        Engine(directory).resume(workflow_id)
    stages.append(engine.snapshot(workflow_id, "Repeated delivery requests remain idempotent"))
    final = stages[-1]
    checks = {"real_worker_exit_code": child.returncode, "reservation_survived_crash": stages[1]["reservation_count"] == 1, "checkpoint_lagged_tool_commit": stages[1]["state"] == "CREATED", "one_reservation": final["reservation_count"] == 1, "stock_decremented_once": final["stock_remaining"] == 4, "approval_enforced": denied, "one_dispatch": final["dispatch_count"] == 1, "completed": final["state"] == "DISPATCHED"}
    report = {"version": "0.1", "run_id": run_id, "workflow_id": workflow_id, "kind": "Recorded deterministic workflow with an actual subprocess hard exit", "stages": stages, "checks": checks, "limitations": LIMITATIONS}
    target = output / "demo.json"
    target.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(target.resolve())
    return 0 if all(value is True or key == "real_worker_exit_code" for key, value in checks.items()) else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description="Workflow Recovery local prototype")
    commands = parser.add_subparsers(dest="command", required=True)
    command = commands.add_parser("demo", help="Run a real crash/resume demonstration")
    command.add_argument("--output", type=Path, default=Path(__file__).resolve().parents[2] / "artifacts")
    for name in ("create", "worker", "approve", "dispatch", "inspect"):
        command = commands.add_parser(name)
        command.add_argument("--directory", type=Path, required=True)
        command.add_argument("--id", required=True)
        if name == "worker":
            command.add_argument("--crash-after-reservation", action="store_true")
            command.add_argument("--crash-after-dispatch", action="store_true")
        if name == "approve":
            command.add_argument("--actor", required=True)
    args = parser.parse_args(argv)
    if args.command == "demo":
        return demo(args.output)
    engine = Engine(args.directory)
    if args.command == "create":
        engine.create(args.id)
    elif args.command == "worker":
        engine.resume(args.id, args.crash_after_reservation, args.crash_after_dispatch)
    elif args.command == "approve":
        engine.approve(args.id, args.actor)
    elif args.command == "dispatch":
        engine.dispatch(args.id)
    print(json.dumps(engine.snapshot(args.id, "Current persisted state"), indent=2))
    return 0
