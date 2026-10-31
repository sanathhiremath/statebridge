# StateBridge

Version 0.1.

A working local demonstration of a workflow surviving a worker crash after a tool has committed its side effect, but before the workflow has saved its acknowledgment.

This is a deterministic shipment-replacement workflow. It does not use a language model, ship real goods, or authenticate a human reviewer. Those boundaries are explicit in the exported evidence.

## Run on Windows or Ubuntu

Python 3.11+ and the standard library are sufficient for the demo:

```text
python run.py demo
```

The command creates a unique pair of databases under `artifacts/runs/`, launches a subprocess that hard-exits with code23 immediately after a committed reservation, reopens the databases, resumes, verifies the approval gate, approves, and dispatches. It then repeats execution to verify there is still one reservation and one dispatch. The inspectable record is `artifacts/demo.json`.

For tests, install development dependencies into a virtual environment:

```text
python -m venv .venv
```

Windows: `.venv\Scripts\python -m pip install -e ".[dev]"`, then `.venv\Scripts\python -m pytest`.

Ubuntu: `.venv/bin/python -m pip install -e '.[dev]'`, then `.venv/bin/python -m pytest`.

## Explore persisted state yourself

```text
python run.py create --directory artifacts/manual --id shipment-1042
python run.py worker --directory artifacts/manual --id shipment-1042 --crash-after-reservation
python run.py inspect --directory artifacts/manual --id shipment-1042
python run.py worker --directory artifacts/manual --id shipment-1042
python run.py approve --directory artifacts/manual --id shipment-1042 --actor demo-reviewer
python run.py dispatch --directory artifacts/manual --id shipment-1042
```

The injected worker command deliberately exits with code23. Its inventory transaction remains committed while its workflow transaction rolls back. Calling `dispatch` before approval raises `ApprovalRequired`.

## Architecture and decision

`workflows.sqlite3` contains state transitions, actor approval, and an append-only event journal. `inventory.sqlite3` contains stock, reservations, and dispatches. Keeping these stores separate makes the commit/acknowledgment gap real. A workflow ID is the idempotency key for both sandbox tools. Tool effects commit independently; retries consult the tool's unique key rather than assuming a missing checkpoint means the action never happened.

SQLite `BEGIN IMMEDIATE` serializes local workflow transitions. Inventory updates, stock decrement, and reservation insertion share one tool transaction. Conflicting reuse of a workflow ID with another SKU is rejected. Approval is durable; dispatch is forbidden until that approval is recorded. Dispatch also has a unique workflow key and its lost-acknowledgment gap is tested using a second hard process exit.

This demonstrates recovery semantics for local databases, not an exactly-once guarantee across arbitrary remote tools. A real integration needs independently enforced tool idempotency, authenticated approvals, access controls, retention, reconciliation, and operator recovery paths.

## Portfolio artifact contract

`artifacts/demo.json` contains `version`, `run_id`, `workflow_id`, `kind`, `stages`, `checks`, and `limitations`. Each stage has `label`, `state`, `reservation_count`, `stock_remaining`, `dispatch_count`, `approver`, and persisted `events`. The portfolio can replay these snapshots without implying that visiting the page starts a worker or reserves inventory.

## Repository

Source: https://github.com/sanathhiremath/statebridge


This repository contains a local working prototype. Reproduction commands, measured scope, and limitations are documented above.
