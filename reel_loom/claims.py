from .errors import LoomError, require, missing
from .probe import inspect_media
from .rendering import check_rendition
from .values import identifier, now, object_body, text, token, number


class Claims:
    """Leased remote execution: workers register, claim frozen render tickets,
    heartbeat while encoding, and commit media back. A ticket admits exactly one
    winning commit — the holder of the current lease; expired leases are relayed
    to other workers and late results are rejected without touching stored facts."""

    def __init__(self, db, settings, tickets, blobs, artifacts):
        self.db, self.settings, self.tickets = db, settings, tickets
        self.blobs, self.artifacts = blobs, artifacts

    def register(self, body):
        object_body(body, ("label",), ("label",))
        label = text(body["label"], "label", 100)
        with self.db.write_unit() as db:
            worker_id, moment = identifier("worker"), now()
            db.execute("INSERT INTO workers VALUES(?,?,?,?)", (worker_id, label, moment, moment))
            self.db.event(db, worker_id, "worker_registered", {"label": label})
            return {**self.worker(worker_id, db), "lease_seconds": self.settings.lease_seconds}

    def worker(self, worker_id, db=None):
        if db is None:
            with self.db.read() as read:
                return self.worker(worker_id, read)
        row = db.execute("SELECT * FROM workers WHERE id=?", (worker_id,)).fetchone()
        if row is None:
            raise missing("worker", worker_id)
        return dict(row)

    def workers(self):
        with self.db.read() as db:
            return [dict(row) for row in db.execute("SELECT * FROM workers ORDER BY created,id")]

    def claim(self, worker_id, body):
        object_body(body, ())
        with self.db.write_unit() as db:
            self.worker(worker_id, db)
            db.execute("UPDATE workers SET seen=? WHERE id=?", (now(), worker_id))
        ticket = self.tickets.claim(holder="worker:"+worker_id, lease_seconds=self.settings.lease_seconds)
        if ticket is None:
            return {"claim": None}
        return {"claim": ticket["lease"]["claim"], "ticket": ticket,
                "lease_expires": ticket["lease"]["expires"],
                "heartbeat_seconds": max(.5, self.settings.lease_seconds/3)}

    def heartbeat(self, claim_id, body):
        object_body(body, ("progress",))
        progress = body.get("progress")
        if progress is not None:
            progress = number(progress, "progress", 0, 1)
        return self.tickets.heartbeat(claim_id, self.settings.lease_seconds, progress)

    def fail(self, claim_id, body):
        object_body(body, ("error", "detail"), ("error",))
        code = token(body["error"], "error")
        detail = body.get("detail", "Worker reported failure")
        require(isinstance(detail, str), "invalid_text", "detail must be text")
        return self.tickets.fail_claim(claim_id, LoomError(code, detail[:400], 422))

    def commit(self, claim_id, raw):
        require(len(raw) > 0, "empty_output", "Media output is empty", 422)
        with self.db.read() as db:
            row = self.tickets.claim_row(db, claim_id)
        ticket = self.tickets.get(row["ticket"])
        kind = ticket["snapshot"]["look"]["spec"]["kind"]
        with self.blobs.temporary(".mp4" if kind == "video" else ".wav") as output:
            output.write_bytes(raw)
            metadata = inspect_media(output, self.settings)
            check_rendition(ticket["snapshot"], metadata)
            try:
                artifact = self.artifacts.commit_render(
                    ticket["id"], output, {**metadata, "fingerprint": ticket["fingerprint"]}, claim_id)
            except LoomError as exc:
                # A losing commit changes nothing, but the attempt itself is a fact:
                # record it so the ticket history shows the late or cancelled result.
                with self.db.write_unit() as db:
                    self.db.event(db, ticket["id"], "commit_rejected",
                                  {"claim": claim_id, "reason": exc.code})
                raise
        return {"artifact": artifact, "ticket": self.tickets.get(ticket["id"])}
