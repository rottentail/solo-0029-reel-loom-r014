"""A bounded production walkthrough using generated audiovisual media and HTTP."""
import argparse
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
from .config import Settings
from .application import Application
from .api import Api
from .http import MediaHttpServer
from .client import Client
from .errors import LoomError, require

WORKFLOWS = ("ingest", "revisions", "looks", "render", "signal", "waveform", "loudness",
             "sheet", "captions", "imprint", "cancel-retry", "recovery", "resources", "matrix",
             "conformance", "lineage", "remote-relay")


def expect_error(action, code):
    try:
        action()
    except LoomError as exc:
        require(exc.code == code, "walkthrough_error", "Unexpected workflow rejection", expected=code, actual=exc.code)
        return exc.as_dict()
    raise RuntimeError("The walkthrough expected an operation to be rejected: " + code)


class Walkthrough:
    def __init__(self, settings):
        self.settings = settings
        self.app = Application(settings, start_workers=False)
        self.server = MediaHttpServer(("127.0.0.1", 0), Api(self.app))
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": .02}, daemon=True)
        self.thread.start()
        self.client = Client(f"http://127.0.0.1:{self.server.server_port}/loom/v1")

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.app.close()

    def setup(self):
        with self.app.blobs.temporary(".mp4") as sample:
            self.app.processes.run(["-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25:duration=3",
                                    "-f", "lavfi", "-i", "sine=frequency=659:sample_rate=48000:duration=3",
                                    "-af", "volume=enable='between(t,1,1.5)':volume=0",
                                    "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", "-shortest", str(sample)])
            self.content = sample.read_bytes()
        self.asset = self.client.ingest(self.content, "harbor-tone")
        self.slices = [{"asset": self.asset["id"], "in": .2, "out": 1.2, "label": "开场"},
                       {"asset": self.asset["id"], "in": 1.6, "out": 2.8, "label": "收束"}]
        self.reel = self.client.create_reel("回声样片", self.slices, "demo-reel")
        self.look = self.client.put_look("preview", {"kind": "video", "width": 320, "height": 180})
        self.audio_look = self.client.put_look("listening", {"kind": "audio", "sample_rate": 48000})
        self.ticket = self.client.render(self.reel["id"], "preview", key="demo-render")
        return self

    def finish(self):
        ticket = self.client.request("GET", "/tickets/"+self.ticket["id"])
        if ticket["state"] == "queued":
            self.app.renderer.run_once()
        ticket = self.client.wait(self.ticket["id"], timeout=5)
        require(ticket["state"] == "succeeded", "walkthrough_render", "Walkthrough render did not succeed", ticket=ticket)
        return ticket

    def run(self, name):
        c, a = self.client, self.asset["id"]
        if name == "ingest":
            duplicate = c.ingest(self.content, "duplicate-name")
            require(duplicate["id"] == a, "walkthrough_dedup", "Identical ready media should be reused")
            failure = expect_error(lambda: c.ingest(b"not-media", "bad"), "unreadable_media")
            return {"asset": self.asset, "deduplicated": True, "failure": failure}
        if name == "revisions":
            updated = c.replace_reel(self.reel["id"], 1, "回声样片·修订", list(reversed(self.slices)))
            old = c.request("GET", "/reels/"+self.reel["id"]+"?revision=1")
            require(old["hash"] == self.reel["hash"] and updated["revision"] == 2,
                    "walkthrough_revision", "Historical reel changed")
            failure = expect_error(lambda: c.replace_reel(self.reel["id"], 1, "旧稿", self.slices), "revision_conflict")
            return {"head": updated["head"], "historical_hash": old["hash"], "failure": failure}
        if name == "looks":
            new = c.put_look("preview", {"kind": "video", "width": 640, "height": 360}, 1)
            ticket = c.request("GET", "/tickets/"+self.ticket["id"])
            require(ticket["look_version"] == 1 and new["version"] == 2, "walkthrough_look", "Queued look drifted")
            failure = expect_error(lambda: c.put_look("preview", {"kind":"video"}, 1), "revision_conflict")
            return {"new_version": 2, "frozen_version": ticket["look_version"], "failure": failure}
        if name == "render":
            ticket = self.finish()
            body = {"reel": self.reel["id"], "look": "preview"}
            replay = c.request("POST", "/tickets", body, {"Idempotency-Key":"demo-render"})
            require(replay["id"] == ticket["id"], "walkthrough_idempotency", "Render replay changed identity")
            failure = expect_error(lambda: c.request("POST", "/tickets", {**body,"look":"listening"},
                                                   {"Idempotency-Key":"demo-render"}), "idempotency_conflict")
            audio = c.render(self.reel["id"], "listening")
            self.app.renderer.run_once()
            audio = c.wait(audio["id"], 5)
            require(audio["state"] == "succeeded", "walkthrough_audio", "Audio rendition did not succeed")
            return {"video": ticket, "audio": audio, "failure": failure}
        if name in ("signal", "waveform", "loudness"):
            result = c.request("POST", f"/assets/{a}/{name}", {})
            replay = c.request("POST", f"/assets/{a}/{name}", {})
            require(result["id"] == replay["id"], "walkthrough_analysis", "Analysis cache should be content/settings stable")
            failure = expect_error(lambda: c.request("POST", f"/assets/{a}/{name}", {"unsupported": 1}), "unknown_fields")
            return {"analysis": result, "cached": True, "failure": failure}
        if name == "sheet":
            result = c.request("POST", f"/assets/{a}/contact-sheet", {"count": 6, "columns": 3})
            media = c.request("GET", "/artifacts/"+result["id"]+"/content", raw=True)
            require(media.startswith(b"\xff\xd8"), "walkthrough_sheet", "Contact sheet is not JPEG")
            failure = expect_error(lambda: c.request("POST", f"/assets/{a}/contact-sheet", {"count":1,"columns":3}), "sheet_layout")
            return {"artifact": result, "bytes": len(media), "failure": failure}
        if name == "captions":
            track = c.request("POST", f"/assets/{a}/cues", {"language":"zh-CN",
                              "srt":"1\n00:00:00,000 --> 00:00:02,900\n潮声\n"})
            projected = c.request("POST", f'/reels/{self.reel["id"]}/captions', {"revision":1,"tracks":[track["id"]]})
            require(len(projected["cues"]) == 2, "walkthrough_captions", "A cue spanning cuts must project to both slices")
            failure = expect_error(lambda: c.request("POST", f"/assets/{a}/cues", {"language":"zh","srt":"broken"}), "cue_format")
            return {"projection": projected, "track": track, "failure": failure}
        if name == "imprint":
            failure = expect_error(lambda: c.imprint(self.ticket["id"]), "imprint_state")
            ticket = self.finish()
            imprint = c.imprint(ticket["id"])
            verified = c.request("GET", f'/imprints/{imprint["id"]}/verify')
            self.client.replace_reel(self.reel["id"], 1, "新的草剪", list(reversed(self.slices)))
            unchanged = c.imprint(ticket["id"])
            require(imprint["id"] == unchanged["id"], "walkthrough_imprint", "Upstream editing changed frozen imprint")
            return {"imprint": imprint, "verified": verified, "failure": failure}
        if name == "cancel-retry":
            cancelled = c.request("POST", f'/tickets/{self.ticket["id"]}/cancel')
            require(cancelled["state"] == "cancelled", "walkthrough_cancel", "Queued render did not cancel")
            retried = c.request("POST", f'/tickets/{self.ticket["id"]}/retry')
            ticket = self.finish()
            failure = expect_error(lambda: c.request("POST", f'/tickets/{self.ticket["id"]}/retry'), "retry_state")
            require(retried["attempt"] == 2, "walkthrough_attempt", "Retry attempt was not incremented")
            return {"ticket": ticket, "failure": failure}
        if name == "recovery":
            claimed = self.app.tickets.claim()
            self.close()
            with Application(self.settings, start_workers=False) as restarted:
                recovered = restarted.tickets.get(claimed["id"])
                require(recovered["state"] == "failed" and recovered["error"]["error"] == "worker_interrupted",
                        "walkthrough_recovery", "Interrupted render was not recovered")
                restarted.tickets.retry(claimed["id"])
                finished = restarted.renderer.run_once()
                require(finished["state"] == "succeeded", "walkthrough_recovery", "Recovered render could not retry")
                return {"interrupted": recovered, "retried_state": finished["state"]}
        if name == "resources":
            c.request("POST", f"/assets/{a}/retire")
            failure = expect_error(lambda: c.create_reel("退役来源", self.slices), "asset_retired")
            ticket = self.finish()
            sweep = c.request("POST", "/resources/sweep", {"dry_run":False})
            status = c.request("GET", "/resources")
            require(status["consistent"] and not sweep["removed"], "walkthrough_resources", "Referenced source was collected")
            return {"reconcile":status,"sweep":sweep,"render_state":ticket["state"],"failure":failure}
        if name == "matrix":
            body = {"reel":self.reel["id"],"looks":[{"id":"preview"},{"id":"listening"}]}
            matrix = c.request("POST", "/matrices", body, {"Idempotency-Key":"demo-matrix"})
            before = len(c.request("GET", "/tickets")["items"])
            failure = expect_error(lambda: c.request("POST", "/matrices", {**body,"looks":[{"id":"preview"},{"id":"missing-look"}]}), "not_found")
            require(len(c.request("GET", "/tickets")["items"]) == before, "walkthrough_matrix", "Invalid matrix committed partial work")
            while self.app.renderer.run_once() is not None:
                pass
            finished = c.request("GET", "/matrices/"+matrix["id"])
            require(finished["state"] == "succeeded", "walkthrough_matrix", "Matrix did not complete")
            return {"matrix": finished,"failure":failure}
        if name == "conformance":
            ticket = self.finish()
            artifact = ticket["artifacts"][0]
            result = c.request("GET", f'/artifacts/{artifact["id"]}/conformance')
            require(result["valid"], "walkthrough_conformance", "Rendered media violates frozen look", report=result)
            failure = expect_error(lambda: c.request("GET", "/artifacts/missing/conformance"), "not_found")
            return {"conformance":result,"failure":failure}
        if name == "lineage":
            self.finish()
            imprint = c.imprint(self.ticket["id"])
            result = c.request("GET", f'/tickets/{self.ticket["id"]}/lineage')
            require(any(n["id"] == imprint["id"] for n in result["nodes"]), "walkthrough_lineage", "Imprint missing from lineage")
            failure = expect_error(lambda: c.request("GET", "/tickets/missing/lineage"), "not_found")
            return {"lineage":result,"failure":failure}
        if name == "remote-relay":
            return self.remote_relay()
        raise ValueError("Unknown walkthrough: " + name)

    def remote_relay(self):
        c = self.client
        # The setup ticket stays out of the way; only the long-form render is claimed remotely.
        c.request("POST", f'/tickets/{self.ticket["id"]}/cancel')
        # A 1440p render keeps the first worker busy for seconds, long enough to vanish mid-encode.
        with self.app.blobs.temporary(".mp4") as sample:
            self.app.processes.run(["-f", "lavfi", "-i", "testsrc2=size=2560x1440:rate=30:duration=20",
                                    "-f", "lavfi", "-i", "sine=frequency=523:sample_rate=48000:duration=20",
                                    "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", "-shortest", str(sample)])
            content = sample.read_bytes()
        asset = c.ingest(content, "longform-relay")
        slices = [{"asset": asset["id"], "in": 1.0, "out": 10.0}, {"asset": asset["id"], "in": 11.0, "out": 19.0}]
        reel = c.create_reel("长片接力", slices)
        c.put_look("cinema", {"kind": "video", "width": 2560, "height": 1440, "frame_rate": "30"})
        ticket = c.render(reel["id"], "cinema")
        # Frozen inputs must not drift for remote execution: the draft moves on
        # to revision 2 and the source retires after the ticket was enqueued.
        c.replace_reel(reel["id"], 1, "长片接力·新稿", list(reversed(slices)))
        c.request("POST", f"/assets/{asset['id']}/retire")
        env = dict(os.environ)
        env["PYTHONPATH"] = str(Path(__file__).resolve().parent.parent)+os.pathsep+env.get("PYTHONPATH", "")
        command = [sys.executable, "-m", "reel_loom.worker", "--server", c.base_url,
                   "--ffmpeg", self.settings.ffmpeg, "--idle-seconds", "30",
                   "--max-claims", "1", "--timeout", "60"]
        first = subprocess.Popen(command+["--label", "relay-a"], env=env,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        second = None
        try:
            # Wait until the first worker's encode is visibly underway, then it vanishes.
            deadline = time.monotonic()+30
            while True:
                current = c.request("GET", "/tickets/"+ticket["id"])
                if current["state"] == "running" and current["lease"] and current["progress"] > 0:
                    break
                require(time.monotonic() < deadline and first.poll() is None, "walkthrough_remote",
                        "The first worker never started encoding", ticket=current)
                time.sleep(.05)
            holder_a, claim_a = current["lease"]["holder"], current["lease"]["claim"]
            first.send_signal(signal.SIGSTOP)
            second = subprocess.Popen(command+["--label", "relay-b"], env=env,
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out_b, err_b = second.communicate(timeout=120)
            require(second.returncode == 0, "walkthrough_remote", "The relay worker failed",
                    log=err_b.decode(errors="replace")[-600:])
            relayed = c.request("GET", "/tickets/"+ticket["id"])
            require(relayed["state"] == "succeeded" and relayed["attempt"] == 2
                    and len(relayed["artifacts"]) == 1 and relayed["lease"] is None,
                    "walkthrough_remote", "The relay did not finish the render", ticket=relayed)
        finally:
            for process in (first, second):
                if process is not None and process.poll() is None:
                    try:
                        process.send_signal(signal.SIGCONT)
                    except (ProcessLookupError, OSError):
                        pass
            for process in (first, second):
                if process is not None and process.poll() is None:
                    try:
                        process.communicate(timeout=60)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.communicate()
        # The first worker comes back with its stale result; it must not overwrite the relay.
        require(first.wait() == 0, "walkthrough_remote", "The superseded worker did not exit cleanly")
        after = c.request("GET", "/tickets/"+ticket["id"])
        require(after["state"] == "succeeded" and after["artifacts"] == relayed["artifacts"],
                "walkthrough_remote", "A late result overwrote the relayed rendition", ticket=after)
        events = [item["kind"] for item in c.request("GET", f"/tickets/{ticket['id']}/events")["items"]]
        require("relayed" in events and "commit_rejected" in events,
                "walkthrough_remote", "The relay and the rejected late commit are not both audited", events=events)
        # Task detail, downloadable media and delivery imprint must state one final fact.
        artifact = after["artifacts"][0]
        media = c.request("GET", "/artifacts/"+artifact["id"]+"/content", raw=True)
        require(hashlib.sha256(media).hexdigest() == artifact["blob"],
                "walkthrough_remote", "Downloaded media differs from the committed rendition")
        imprint = c.imprint(ticket["id"])
        require(imprint["manifest"]["files"][0]["blob"] == artifact["blob"]
                and imprint["manifest"]["snapshot"]["revision"] == 1,
                "walkthrough_remote", "The imprint disagrees with the ticket facts")
        verified = c.request("GET", f"/imprints/{imprint['id']}/verify")
        require(verified["valid"], "walkthrough_remote", "The imprint does not verify")
        # Cancellation keeps its meaning for leased remote execution. The retired
        # source cannot enter new orchestration, so it is restored first; the
        # already-frozen relay render above was never affected by the retirement.
        c.request("POST", f"/assets/{asset['id']}/restore")
        followup = c.render(reel["id"], "cinema")
        worker_c = c.request("POST", "/workers", {"label": "relay-c"})
        job = c.request("POST", f"/workers/{worker_c['id']}/claims", {})
        require(job["claim"], "walkthrough_remote", "The third worker could not claim")
        c.request("POST", f"/tickets/{followup['id']}/cancel")
        beat = c.request("POST", f"/claims/{job['claim']}/heartbeat", {"progress": .2})
        require(beat["cancel"], "walkthrough_remote", "Cancellation is not visible to the remote worker")
        ended = c.request("POST", f"/claims/{job['claim']}/fail",
                          {"error": "render_cancelled", "detail": "observed cancellation"})
        require(ended["state"] == "cancelled", "walkthrough_remote", "Cancelled remote render did not finalize")
        late = expect_error(lambda: c.request("POST", f"/claims/{job['claim']}/result", media), "render_cancelled")
        return {"ticket": after, "superseded_claim": claim_a, "superseded_holder": holder_a,
                "events": events, "imprint": imprint, "verified": verified,
                "cancel": {"ticket": ended["state"], "heartbeat": beat, "late_commit": late}}


def main():
    parser = argparse.ArgumentParser(description="Run a bounded audiovisual production walkthrough")
    parser.add_argument("--workflow", choices=WORKFLOWS, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="reel-loom-demo-") as folder:
        settings = replace(Settings.from_env(), root=Path(folder), render_timeout=30)
        if args.workflow == "remote-relay":
            settings = replace(settings, lease_seconds=2)
        demo = Walkthrough(settings)
        try:
            demo.setup()
            result = demo.run(args.workflow)
            print(json.dumps({"workflow":args.workflow,"ok":True,"result":result}, ensure_ascii=False))
        finally:
            if args.workflow != "recovery":
                demo.close()


if __name__ == "__main__":
    main()
