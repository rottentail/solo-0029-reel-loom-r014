import json
from .errors import LoomError, require, missing
from .values import identifier, now, canonical, object_body, integer

LOCAL_PREFIX = "local"
WORKER_PREFIX = "worker:"


class Tickets:
    def __init__(self, db, planner, keys):
        self.db, self.planner, self.keys = db, planner, keys

    def enqueue(self, body, key=None):
        object_body(body, ("reel", "revision", "look", "look_version"), ("reel", "look"))
        with self.db.write_unit() as db:
            previous = self.keys.replay(db, "tickets", key, body)
            if previous:
                return previous
            snapshot = self.planner.freeze(db, body["reel"], body.get("revision"),
                                           body["look"], body.get("look_version"))
            ticket_id, created = identifier("render"), now()
            db.execute("INSERT INTO tickets(id,reel,revision,look_id,look_version,snapshot,fingerprint,state,attempt,created,updated) "
                       "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                       (ticket_id, snapshot["reel"], snapshot["revision"], snapshot["look"]["id"],
                        snapshot["look"]["version"], canonical(snapshot), snapshot["fingerprint"], "queued", 1, created, created))
            self.db.event(db, ticket_id, "queued", {"fingerprint": snapshot["fingerprint"]})
            result = self.get(ticket_id, db)
            return self.keys.record(db, "tickets", key, body, result)

    def get(self, ticket_id, db=None):
        if db is None:
            with self.db.read() as read:
                return self.get(ticket_id, read)
        row = db.execute("SELECT * FROM tickets WHERE id=?", (ticket_id,)).fetchone()
        if row is None:
            raise missing("render ticket", ticket_id)
        result = dict(row)
        result["snapshot"] = json.loads(result["snapshot"])
        result["error"] = json.loads(result["error"]) if result["error"] else None
        result["cancel"] = bool(result["cancel"])
        lease = None
        if result["lease_id"]:
            lease = {"claim": result["lease_id"], "holder": result["lease_holder"],
                     "expires": result["lease_expires"]}
        result["lease"] = lease
        for field in ("lease_id", "lease_holder", "lease_expires"):
            del result[field]
        result["artifacts"] = [dict(item) for item in db.execute(
            "SELECT id,kind,blob,metadata FROM artifacts WHERE ticket=? ORDER BY created,id", (ticket_id,))]
        for item in result["artifacts"]:
            item["metadata"] = json.loads(item["metadata"])
        return result

    def list(self, state=None):
        with self.db.read() as db:
            if state:
                rows = db.execute("SELECT id FROM tickets WHERE state=? ORDER BY created DESC,id LIMIT 100", (state,))
            else:
                rows = db.execute("SELECT id FROM tickets ORDER BY created DESC,id LIMIT 100")
            return [self.get(row["id"], db) for row in rows]

    def claim(self, holder=LOCAL_PREFIX, lease_seconds=30):
        with self.db.write_unit() as db:
            moment = now()
            # A vanished worker that owed a cancellation can never report back;
            # its expired lease is where the cancellation becomes final.
            stale = db.execute("SELECT id,lease_holder FROM tickets WHERE state='running' AND cancel=1 "
                               "AND lease_expires IS NOT NULL AND lease_expires<?", (moment,)).fetchall()
            for row in stale:
                db.execute("UPDATE tickets SET state='cancelled',lease_id=NULL,lease_holder=NULL,"
                           "lease_expires=NULL,updated=? WHERE id=?", (moment, row["id"]))
                self.db.event(db, row["id"], "cancelled",
                              {"reason": "lease_expired", "holder": row["lease_holder"]})
            row = db.execute("SELECT id,state,attempt,lease_holder FROM tickets "
                             "WHERE state='queued' OR (state='running' AND cancel=0 AND lease_expires<?) "
                             "ORDER BY created,id LIMIT 1", (moment,)).fetchone()
            if row is None:
                return None
            claim_id, expires = identifier("claim"), moment+lease_seconds
            if row["state"] == "running":
                # Relay: the previous holder lost its lease; only the new claim may commit.
                attempt = row["attempt"]+1
                db.execute("UPDATE tickets SET lease_id=?,lease_holder=?,lease_expires=?,attempt=?,"
                           "progress=0,error=NULL,updated=? WHERE id=? AND state='running' AND lease_expires<?",
                           (claim_id, holder, expires, attempt, moment, row["id"], moment))
                self.db.event(db, row["id"], "relayed",
                              {"holder": holder, "previous": row["lease_holder"],
                               "attempt": attempt, "claim": claim_id})
            else:
                db.execute("UPDATE tickets SET state='running',lease_id=?,lease_holder=?,lease_expires=?,updated=? "
                           "WHERE id=? AND state='queued'", (claim_id, holder, expires, moment, row["id"]))
                self.db.event(db, row["id"], "started", {"holder": holder, "claim": claim_id})
            db.execute("INSERT INTO claims VALUES(?,?,?,?,?)",
                       (claim_id, row["id"], holder, moment, expires))
            return self.get(row["id"], db)

    def heartbeat(self, claim_id, lease_seconds, progress=None):
        with self.db.write_unit() as db:
            row = self.claim_row(db, claim_id)
            require(row["state"] == "running" and row["lease_id"] == claim_id,
                    "lease_lost", "The claim is no longer held", 409)
            expires = now()+lease_seconds
            if progress is None:
                db.execute("UPDATE tickets SET lease_expires=?,updated=? WHERE id=?", (expires, now(), row["ticket"]))
            else:
                db.execute("UPDATE tickets SET progress=MAX(progress,?),lease_expires=?,updated=? WHERE id=?",
                           (min(progress, .99), expires, now(), row["ticket"]))
            db.execute("UPDATE claims SET expires=? WHERE id=?", (expires, claim_id))
            return {"ticket": row["ticket"], "state": "running",
                    "cancel": bool(row["cancel"]), "lease_expires": expires}

    def claim_row(self, db, claim_id):
        row = db.execute("SELECT t.id AS ticket,t.state AS state,t.cancel AS cancel,t.lease_id AS lease_id "
                         "FROM claims c JOIN tickets t ON t.id=c.ticket WHERE c.id=?", (claim_id,)).fetchone()
        if row is None:
            raise missing("claim", claim_id)
        return row

    def cancellation_requested(self, ticket_id):
        with self.db.read() as db:
            row = db.execute("SELECT cancel FROM tickets WHERE id=?", (ticket_id,)).fetchone()
            return row is None or bool(row["cancel"])

    def progress(self, ticket_id, value, claim=None, lease_seconds=None):
        with self.db.write_unit() as db:
            if claim is None:
                db.execute("UPDATE tickets SET progress=MAX(progress,?),updated=? WHERE id=? AND state='running'",
                           (min(value, .99), now(), ticket_id))
                return
            expires = now()+lease_seconds
            cursor = db.execute("UPDATE tickets SET progress=MAX(progress,?),lease_expires=?,updated=? "
                                "WHERE id=? AND state='running' AND lease_id=?",
                                (min(value, .99), expires, now(), ticket_id, claim))
            if cursor.rowcount == 0:
                raise LoomError("lease_lost", "The claim is no longer held", 409)

    def cancel(self, ticket_id):
        with self.db.write_unit() as db:
            ticket = self.get(ticket_id, db)
            if ticket["state"] == "queued":
                db.execute("UPDATE tickets SET state='cancelled',cancel=1,updated=? WHERE id=?", (now(), ticket_id))
            elif ticket["state"] == "running":
                db.execute("UPDATE tickets SET cancel=1,updated=? WHERE id=?", (now(), ticket_id))
            else:
                return ticket
            self.db.event(db, ticket_id, "cancel_requested", {})
            return self.get(ticket_id, db)

    def fail(self, ticket_id, error, claim=None):
        with self.db.write_unit() as db:
            ticket = self.get(ticket_id, db)
            if ticket["state"] != "running":
                return ticket
            if claim is not None and (not ticket["lease"] or ticket["lease"]["claim"] != claim):
                return ticket
            state = "cancelled" if ticket["cancel"] or error.code == "render_cancelled" else "failed"
            db.execute("UPDATE tickets SET state=?,error=?,lease_id=NULL,lease_holder=NULL,lease_expires=NULL,updated=? "
                       "WHERE id=?", (state, canonical(error.as_dict()), now(), ticket_id))
            self.db.event(db, ticket_id, state, error.as_dict())
            return self.get(ticket_id, db)

    def fail_claim(self, claim_id, error):
        with self.db.write_unit() as db:
            row = self.claim_row(db, claim_id)
            if row["state"] != "running" or row["lease_id"] != claim_id:
                return self.get(row["ticket"], db)
            state = "cancelled" if row["cancel"] or error.code == "render_cancelled" else "failed"
            db.execute("UPDATE tickets SET state=?,error=?,lease_id=NULL,lease_holder=NULL,lease_expires=NULL,updated=? "
                       "WHERE id=?", (state, canonical(error.as_dict()), now(), row["ticket"]))
            self.db.event(db, row["ticket"], state, {**error.as_dict(), "claim": claim_id})
            return self.get(row["ticket"], db)

    def retry(self, ticket_id):
        with self.db.write_unit() as db:
            ticket = self.get(ticket_id, db)
            require(ticket["state"] in ("failed", "cancelled"), "retry_state", "Only failed or cancelled renders can retry", 409)
            db.execute("UPDATE tickets SET state='queued',attempt=attempt+1,progress=0,cancel=0,error=NULL,"
                       "lease_id=NULL,lease_holder=NULL,lease_expires=NULL,updated=? WHERE id=?",
                       (now(), ticket_id))
            self.db.event(db, ticket_id, "retried", {"attempt": ticket["attempt"]+1})
            return self.get(ticket_id, db)

    def recover(self):
        with self.db.write_unit() as db:
            rows = db.execute("SELECT id,cancel,lease_holder,lease_expires FROM tickets WHERE state='running'").fetchall()
            moment, interrupted = now(), 0
            for row in rows:
                holder = row["lease_holder"] or LOCAL_PREFIX
                if holder.startswith(WORKER_PREFIX):
                    # Remote leases are governed by expiry and relay, not by service
                    # restart; only an expired lease with a pending cancel is final here.
                    if row["lease_expires"] is not None and row["lease_expires"] <= moment and row["cancel"]:
                        db.execute("UPDATE tickets SET state='cancelled',lease_id=NULL,lease_holder=NULL,"
                                   "lease_expires=NULL,updated=? WHERE id=?", (moment, row["id"]))
                        self.db.event(db, row["id"], "cancelled",
                                      {"reason": "lease_expired", "holder": holder})
                    continue
                state = "cancelled" if row["cancel"] else "failed"
                error = {"error": "worker_interrupted", "detail": "Previous worker stopped before committing output", "context": {}}
                db.execute("UPDATE tickets SET state=?,error=?,lease_id=NULL,lease_holder=NULL,lease_expires=NULL,updated=? "
                           "WHERE id=?", (state, canonical(error), moment, row["id"]))
                self.db.event(db, row["id"], "recovered", {"state": state})
                interrupted += 1
            return {"interrupted": interrupted}
