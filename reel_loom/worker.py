"""Remote render worker: claims frozen render tickets from a 声画织场 service,
encodes them with a local FFmpeg, and commits the media back over HTTP.

The worker only ever sees the frozen ticket snapshot and content-addressed
source blobs; it never re-reads the current reel. If its lease is lost
(expired and relayed, or cancelled), the service rejects the commit and the
worker drops the result — one ticket admits exactly one winning rendition.
"""
import argparse
import hashlib
import logging
from pathlib import Path
import shutil
import sys
import tempfile
import threading
import time
from urllib.request import Request, urlopen
from .client import Client
from .config import Settings
from .errors import LoomError, RenderCancelled
from .planner import compile_args
from .processes import Processes
from .timebase import seconds

logger = logging.getLogger(__name__)


class RemoteWorker:
    def __init__(self, server, label, ffmpeg, render_timeout=180, idle_seconds=6,
                 max_claims=None, scratch=None):
        self.client = Client(server.rstrip("/"))
        self.label, self.ffmpeg = label, ffmpeg
        self.idle_seconds, self.max_claims = idle_seconds, max_claims
        self.scratch = scratch
        self.processes = Processes(Settings(root=Path(scratch or tempfile.gettempdir()),
                                            ffmpeg=ffmpeg, render_timeout=render_timeout))
        self.worker_id = None

    def register(self):
        reply = self.client.request("POST", "/workers", {"label": self.label})
        self.worker_id = reply["id"]
        logger.info("worker %s registered as %s (lease %.1fs)",
                    self.label, self.worker_id, reply["lease_seconds"])
        return reply

    def run(self):
        if self.worker_id is None:
            self.register()
        claimed, idle_since = 0, None
        while True:
            job = self.client.request("POST", f"/workers/{self.worker_id}/claims", {})
            if not job.get("claim"):
                if idle_since is None:
                    idle_since = time.monotonic()
                if time.monotonic()-idle_since >= self.idle_seconds:
                    logger.info("worker %s idle; no more claims", self.label)
                    return 0
                time.sleep(.4)
                continue
            idle_since = None
            claimed += 1
            self.execute(job)
            if self.max_claims and claimed >= self.max_claims:
                return 0

    def execute(self, job):
        claim_id, ticket = job["claim"], job["ticket"]
        snapshot = ticket["snapshot"]
        kind = snapshot["look"]["spec"]["kind"]
        logger.info("worker %s claimed %s (attempt %s)", self.label, ticket["id"], ticket["attempt"])
        with tempfile.TemporaryDirectory(prefix="loom-worker-", dir=self.scratch) as folder:
            folder = Path(folder)
            sources = {}
            for source in snapshot["sources"].values():
                target = folder/source["blob"]
                self.download(source["blob"], target)
                sources[source["blob"]] = target
            output = folder/("media"+(".mp4" if kind == "video" else ".wav"))
            args = compile_args(snapshot, lambda key: sources[key], output)
            stop, lost = threading.Event(), threading.Event()
            shared = {"progress": 0.0}
            pulse = threading.Thread(target=self.pulse,
                                     args=(claim_id, job["heartbeat_seconds"], stop, lost, shared),
                                     daemon=True, name=f"pulse-{claim_id[:12]}")
            pulse.start()
            try:
                self.processes.run(args,
                                   cancelled=lambda: stop.is_set() or lost.is_set(),
                                   progress=lambda value: shared.__setitem__("progress", value),
                                   duration=seconds(snapshot["document"]["duration_tick"]))
            except RenderCancelled:
                if not lost.is_set():
                    self.report_fail(claim_id, "render_cancelled", "The render was cancelled")
                return
            except LoomError as exc:
                if not lost.is_set():
                    self.report_fail(claim_id, exc.code, exc.detail)
                return
            finally:
                stop.set()
                pulse.join(timeout=3)
            try:
                reply = self.client.request("POST", f"/claims/{claim_id}/result", output.read_bytes())
                logger.info("worker %s committed %s as %s",
                            self.label, ticket["id"], reply["artifact"]["id"])
            except LoomError as exc:
                if exc.code == "render_cancelled":
                    self.report_fail(claim_id, "render_cancelled", "The render was cancelled")
                elif exc.code in ("lease_lost", "ticket_state"):
                    logger.info("worker %s lost %s: %s (result dropped)", self.label, ticket["id"], exc.code)
                elif exc.status == 422:
                    self.report_fail(claim_id, exc.code, exc.detail)
                else:
                    raise

    def pulse(self, claim_id, interval, stop, lost, shared):
        while not stop.wait(max(.2, interval)):
            try:
                reply = self.client.request("POST", f"/claims/{claim_id}/heartbeat",
                                            {"progress": shared["progress"]})
            except LoomError as exc:
                if exc.code == "lease_lost":
                    lost.set()
                    return
                continue
            except (ConnectionError, OSError):
                continue  # the service may be restarting; the lease is wall-clock
            if reply.get("cancel"):
                stop.set()
                return

    def report_fail(self, claim_id, code, detail):
        try:
            self.client.request("POST", f"/claims/{claim_id}/fail",
                                {"error": code, "detail": detail})
        except LoomError as exc:
            logger.info("failure report for claim %s was not accepted: %s", claim_id, exc.code)

    def download(self, digest, target):
        request = Request(f"{self.client.base_url}/blobs/{digest}")
        hasher = hashlib.sha256()
        with urlopen(request, timeout=120) as response, target.open("wb") as out:
            for chunk in iter(lambda: response.read(1024*1024), b""):
                hasher.update(chunk)
                out.write(chunk)
        if hasher.hexdigest() != digest:
            raise LoomError("blob_corrupt", "Downloaded source differs from the frozen digest", 409)


def main():
    logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser(description="Run a remote render worker for a 声画织场 service")
    parser.add_argument("--server", default="http://127.0.0.1:8729/loom/v1", help="service API base URL")
    parser.add_argument("--label", default="worker", help="worker label shown in claims and events")
    parser.add_argument("--ffmpeg", default=None, help="FFmpeg executable (default: PATH or imageio-ffmpeg)")
    parser.add_argument("--scratch", default=None, help="directory for worker scratch space")
    parser.add_argument("--timeout", type=int, default=180, help="per-render FFmpeg deadline in seconds")
    parser.add_argument("--idle-seconds", type=float, default=6, help="exit after this long without a claim")
    parser.add_argument("--max-claims", type=int, default=None, help="exit after this many claimed renders")
    args = parser.parse_args()
    ffmpeg = args.ffmpeg or shutil.which("ffmpeg")
    if not ffmpeg:
        try:
            import imageio_ffmpeg
            ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
        except ImportError:
            parser.error("FFmpeg is unavailable; pass --ffmpeg or set LOOM_FFMPEG")
    worker = RemoteWorker(args.server, args.label, ffmpeg, render_timeout=args.timeout,
                          idle_seconds=args.idle_seconds, max_claims=args.max_claims,
                          scratch=args.scratch)
    try:
        sys.exit(worker.run())
    except KeyboardInterrupt:
        logging.info("worker %s stopping; uncommitted claims expire with their lease", args.label)
        sys.exit(0)


if __name__ == "__main__":
    main()
