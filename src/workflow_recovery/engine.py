"""Two independent stores expose the commit/acknowledgment failure gap."""
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
import os
import sqlite3


class ConflictError(ValueError):
    pass


class ApprovalRequired(PermissionError):
    pass


@contextmanager
def transaction(path):
    connection = sqlite3.connect(path, timeout=20, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    try:
        connection.execute("BEGIN IMMEDIATE")
        yield connection
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def event(connection, workflow_id, action):
    connection.execute("INSERT INTO events(workflow_id,action,created_at) VALUES(?,?,?)", (workflow_id, action, datetime.now(timezone.utc).isoformat()))


class Engine:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.workflow_path = self.directory / "workflows.sqlite3"
        self.inventory_path = self.directory / "inventory.sqlite3"
        with transaction(self.workflow_path) as c:
            c.execute("CREATE TABLE IF NOT EXISTS workflows(id TEXT PRIMARY KEY, sku TEXT NOT NULL, state TEXT NOT NULL, approver TEXT)")
            c.execute("CREATE TABLE IF NOT EXISTS events(sequence INTEGER PRIMARY KEY, workflow_id TEXT NOT NULL REFERENCES workflows(id), action TEXT NOT NULL, created_at TEXT NOT NULL)")
        with transaction(self.inventory_path) as c:
            c.execute("CREATE TABLE IF NOT EXISTS stock(sku TEXT PRIMARY KEY, remaining INTEGER NOT NULL CHECK(remaining>=0))")
            c.execute("CREATE TABLE IF NOT EXISTS reservations(workflow_id TEXT PRIMARY KEY, sku TEXT NOT NULL REFERENCES stock(sku))")
            c.execute("CREATE TABLE IF NOT EXISTS dispatches(workflow_id TEXT PRIMARY KEY REFERENCES reservations(workflow_id), approval_actor TEXT NOT NULL)")
            c.execute("INSERT OR IGNORE INTO stock VALUES('replacement-kit',5)")

    def create(self, workflow_id, sku="replacement-kit"):
        if not workflow_id or not sku:
            raise ValueError("workflow ID and SKU must be nonempty")
        with transaction(self.workflow_path) as c:
            existing = c.execute("SELECT * FROM workflows WHERE id=?", (workflow_id,)).fetchone()
            if existing:
                if existing["sku"] != sku:
                    raise ConflictError("Workflow ID reused with a different SKU")
                return
            c.execute("INSERT INTO workflows VALUES(?,?,'CREATED',NULL)", (workflow_id, sku))
            event(c, workflow_id, "replacement_requested")

    def _reserve(self, workflow_id, sku):
        with transaction(self.inventory_path) as c:
            existing = c.execute("SELECT sku FROM reservations WHERE workflow_id=?", (workflow_id,)).fetchone()
            if existing:
                if existing["sku"] != sku:
                    raise ConflictError("Reservation key reused with a different SKU")
                return
            updated = c.execute("UPDATE stock SET remaining=remaining-1 WHERE sku=? AND remaining>0", (sku,))
            if updated.rowcount != 1:
                raise ValueError("No replacement stock available")
            c.execute("INSERT INTO reservations VALUES(?,?)", (workflow_id, sku))

    def resume(self, workflow_id, crash_after_reservation=False, crash_after_dispatch=False):
        # Serialize local transitions. Tool commits are independent of checkpoints.
        with transaction(self.workflow_path) as c:
            workflow = c.execute("SELECT * FROM workflows WHERE id=?", (workflow_id,)).fetchone()
            if not workflow:
                raise KeyError(workflow_id)
            if workflow["state"] == "CREATED":
                self._reserve(workflow_id, workflow["sku"])
                if crash_after_reservation:
                    os._exit(23)
                c.execute("UPDATE workflows SET state='AWAITING_APPROVAL' WHERE id=?", (workflow_id,))
                event(c, workflow_id, "reservation_acknowledged")
            elif workflow["state"] == "APPROVED":
                self._dispatch(workflow_id, workflow["approver"])
                if crash_after_dispatch:
                    os._exit(24)
                c.execute("UPDATE workflows SET state='DISPATCHED' WHERE id=?", (workflow_id,))
                event(c, workflow_id, "dispatch_acknowledged")
            return c.execute("SELECT state FROM workflows WHERE id=?", (workflow_id,)).fetchone()["state"]

    def approve(self, workflow_id, actor):
        if not actor.strip():
            raise ValueError("Approval requires a nonempty actor")
        with transaction(self.workflow_path) as c:
            workflow = c.execute("SELECT * FROM workflows WHERE id=?", (workflow_id,)).fetchone()
            if not workflow:
                raise KeyError(workflow_id)
            if workflow["state"] in ("APPROVED", "DISPATCHED"):
                if workflow["approver"] != actor:
                    raise ConflictError("Approval already recorded by a different actor")
                return
            if workflow["state"] != "AWAITING_APPROVAL":
                raise ApprovalRequired("Reservation must be acknowledged before approval")
            c.execute("UPDATE workflows SET state='APPROVED',approver=? WHERE id=?", (actor, workflow_id))
            event(c, workflow_id, "approved_by:" + actor)

    def dispatch(self, workflow_id):
        with transaction(self.workflow_path) as c:
            workflow = c.execute("SELECT * FROM workflows WHERE id=?", (workflow_id,)).fetchone()
            if not workflow:
                raise KeyError(workflow_id)
            if workflow["state"] not in ("APPROVED", "DISPATCHED"):
                raise ApprovalRequired("Dispatch requires recorded approval")
        return self.resume(workflow_id)

    def _dispatch(self, workflow_id, actor):
        if not actor:
            raise ApprovalRequired("Dispatch requires recorded approval")
        with transaction(self.inventory_path) as c:
            c.execute("INSERT OR IGNORE INTO dispatches VALUES(?,?)", (workflow_id, actor))

    def snapshot(self, workflow_id, label):
        with transaction(self.workflow_path) as c:
            workflow = c.execute("SELECT * FROM workflows WHERE id=?", (workflow_id,)).fetchone()
            if not workflow:
                raise KeyError(workflow_id)
            events = [dict(row) for row in c.execute("SELECT sequence,action,created_at FROM events WHERE workflow_id=? ORDER BY sequence", (workflow_id,))]
        with transaction(self.inventory_path) as c:
            reservation_count = c.execute("SELECT COUNT(*) FROM reservations WHERE workflow_id=?", (workflow_id,)).fetchone()[0]
            dispatch_count = c.execute("SELECT COUNT(*) FROM dispatches WHERE workflow_id=?", (workflow_id,)).fetchone()[0]
            stock = c.execute("SELECT remaining FROM stock WHERE sku=?", (workflow["sku"],)).fetchone()
        return {"label": label, "state": workflow["state"], "reservation_count": reservation_count, "stock_remaining": stock[0] if stock else None, "dispatch_count": dispatch_count, "approver": workflow["approver"], "events": events}
