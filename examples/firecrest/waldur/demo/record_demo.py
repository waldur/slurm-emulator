# /// script
# requires-python = ">=3.10"
# dependencies = ["playwright~=1.52.0", "requests>=2.31"]
# ///
r"""Record a video demo of the Waldur HPC workspace on this stack.

A researcher (alice) signs in through Keycloak, looks at her Climate
allocation (jobs, usage forecast, cluster, files with their plots and
reports, a job's performance), then hands the work to the AI assistant:
how much allocation is left, what a job will cost, a program it writes and
runs on the cluster, why a job failed, how to right-size jobs, what the
coming maintenance means for a long job, and a plot of a result file.

Everything shown is live: Waldur -> FireCREST -> the emulator, and the
emulator runs each batch script (SLURM_EMULATOR_JOB_EXECUTE=1), so the
program the assistant writes really runs and the failing job really fails.

Prerequisites: the stack from ../README.md is up and Waldur is seeded with
AI_ASSISTANT_CREDS (the assistant on). Chromium for Playwright, once:

    uvx --from playwright==1.52.0 playwright install chromium

Run (homeport and mastermind URLs of your dev stack):

    uv run record_demo.py --homeport-url http://localhost:8001 \
        --waldur-url http://localhost:8000 --out hpc-demo.mp4

    # only some scenes (login always runs), e.g. to retake one:
    uv run record_demo.py ... --scenes files,ai-debug

Frames come from the browser's screencast (sharper than Playwright's own
video), and are assembled with ffmpeg at their real timing, except while
the assistant is thinking or a job is running: those stretches play
--speedup times faster, marked with a badge in the corner.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import random
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

from playwright.sync_api import Locator, Page, sync_playwright

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import submit_workload  # Keycloak login and job specs

WIDTH, HEIGHT = 1920, 1080

# Captions, a visible cursor with a click ripple, and the speed-up badge,
# drawn over the page and kept through navigations.
OVERLAY = r"""
(() => {
  if (window.__demo) return;
  const css = `
    #demo-caption { position: fixed; top: 10px; left: 50%; transform: translateX(-50%);
      z-index: 2147483647; pointer-events: none; max-width: 58vw; padding: 10px 22px;
      border-radius: 12px; background: rgba(17, 24, 39, .88); color: #fff;
      font: 500 20px/1.35 Inter, system-ui, sans-serif; text-align: center;
      box-shadow: 0 6px 24px rgba(0,0,0,.25); transition: opacity .35s; opacity: 0; }
    #demo-caption small { display: block; font-weight: 400; font-size: 15px; opacity: .8; margin-top: 2px; }
    #demo-cursor { position: fixed; z-index: 2147483647; pointer-events: none; width: 22px; height: 22px;
      margin: -11px 0 0 -11px; border-radius: 50%; background: rgba(239, 68, 68, .35);
      border: 2px solid rgba(239, 68, 68, .9); transition: transform .12s; left: -50px; top: -50px; }
    #demo-cursor.down { transform: scale(.7); }
    .demo-ripple { position: fixed; z-index: 2147483646; pointer-events: none; width: 16px; height: 16px;
      margin: -8px 0 0 -8px; border-radius: 50%; border: 3px solid rgba(239, 68, 68, .8);
      animation: demo-ripple .6s ease-out forwards; }
    @keyframes demo-ripple { to { transform: scale(4); opacity: 0; } }
    #demo-badge { position: fixed; bottom: 18px; right: 18px; z-index: 2147483647; pointer-events: none;
      padding: 6px 14px; border-radius: 999px; background: rgba(17, 24, 39, .85); color: #fff;
      font: 600 16px/1.2 Inter, system-ui, sans-serif; display: none; }
  `;
  const install = () => {
    if (document.getElementById('demo-caption')) return;
    const style = document.createElement('style');
    style.textContent = css;
    document.head.appendChild(style);
    for (const id of ['demo-caption', 'demo-cursor', 'demo-badge']) {
      const el = document.createElement('div');
      el.id = id;
      document.body.appendChild(el);
    }
  };
  const el = (id) => { install(); return document.getElementById(id); };
  window.__demo = {
    caption(text, sub) {
      const c = el('demo-caption');
      if (!text) { c.style.opacity = 0; return; }
      c.innerHTML = '';
      c.appendChild(document.createTextNode(text));
      if (sub) { const s = document.createElement('small'); s.textContent = sub; c.appendChild(s); }
      c.style.opacity = 1;
    },
    badge(text) { const b = el('demo-badge'); b.textContent = text || ''; b.style.display = text ? 'block' : 'none'; },
  };
  addEventListener('mousemove', (e) => { const c = el('demo-cursor'); c.style.left = e.clientX + 'px'; c.style.top = e.clientY + 'px'; }, true);
  addEventListener('mousedown', (e) => {
    el('demo-cursor').classList.add('down');
    const r = document.createElement('div'); r.className = 'demo-ripple';
    r.style.left = e.clientX + 'px'; r.style.top = e.clientY + 'px';
    document.body.appendChild(r); setTimeout(() => r.remove(), 700);
  }, true);
  addEventListener('mouseup', () => el('demo-cursor').classList.remove('down'), true);
  if (document.readyState === 'loading') addEventListener('DOMContentLoaded', install); else install();
})();
"""

# Scroll the newest assistant answer into view from its top, then down to
# its end at reading pace. Resolves when done.
READ_ANSWER = """
async ([pxPerSecond, minSeconds, maxSeconds]) => {
  const view = document.querySelector('.aui-thread-viewport');
  const answers = document.querySelectorAll('.aui-assistant-message-root');
  if (!view || !answers.length) return 0;
  const last = answers[answers.length - 1];
  const glide = (to, ms) => new Promise((done) => {
    const from = view.scrollTop, t0 = performance.now();
    const step = (t) => {
      const k = Math.min(1, (t - t0) / ms);
      view.scrollTop = from + (to - from) * (k < .5 ? 2 * k * k : 1 - (-2 * k + 2) ** 2 / 2);
      k < 1 ? requestAnimationFrame(step) : done();
    };
    requestAnimationFrame(step);
  });
  const top = last.offsetTop - view.offsetTop - 12;
  const bottom = view.scrollHeight - view.clientHeight;
  await glide(Math.max(0, Math.min(top, bottom)), 700);
  await new Promise((r) => setTimeout(r, 2500));
  const distance = Math.max(0, bottom - view.scrollTop);
  const seconds = Math.min(maxSeconds, Math.max(minSeconds, distance / pxPerSecond));
  if (distance > 0) await glide(bottom, seconds * 1000);
  return seconds;
}
"""

TERMINAL = {"COMPLETED", "FAILED", "TIMEOUT", "CANCELLED", "OUT_OF_MEMORY", "NODE_FAIL"}


@dataclass
class Recorder:
    """Screencast frames plus the stretches to play faster."""

    frames_dir: Path
    frames: list[tuple[float, Path]] = field(default_factory=list)
    fast: list[tuple[float, float]] = field(default_factory=list)
    _fast_since: float | None = None

    def on_frame(self, cdp, params: dict) -> None:
        path = self.frames_dir / f"{len(self.frames):06d}.jpg"
        path.write_bytes(base64.b64decode(params["data"]))
        self.frames.append((params["metadata"]["timestamp"], path))
        cdp.send("Page.screencastFrameAck", {"sessionId": params["sessionId"]})

    def fast_start(self) -> None:
        self._fast_since = time.time()

    def fast_stop(self) -> None:
        if self._fast_since is not None:
            self.fast.append((self._fast_since, time.time()))
            self._fast_since = None

    def is_fast(self, t: float) -> bool:
        return any(a <= t < b for a, b in self.fast)


class Demo:
    def __init__(self, args: argparse.Namespace, page: Page, rec: Recorder):
        self.args = args
        self.page = page
        self.rec = rec
        self.url = args.homeport_url.rstrip("/")
        self.waldur = submit_workload.Waldur(
            argparse.Namespace(
                waldur_url=args.waldur_url,
                public_url=None,
                route=[],
                password=args.password,
                relogin_minutes=50,
                token_dir=None,
            )
        )
        self.resource_uuid = ""
        self.failed_job = ""
        self.program_job = ""

    # --- pacing and pointer -------------------------------------------------

    def hold(self, seconds: float) -> None:
        # Never time.sleep: the screencast frames arrive only while
        # Playwright is waiting on the browser.
        self.page.wait_for_timeout(seconds * 1000)

    def caption(self, text: str = "", sub: str = "") -> None:
        self.page.evaluate("([t, s]) => window.__demo && window.__demo.caption(t, s)", [text, sub])

    def badge(self, text: str = "") -> None:
        self.page.evaluate("(t) => window.__demo && window.__demo.badge(t)", text)

    def point(self, target: Locator) -> None:
        target.scroll_into_view_if_needed()
        box = target.bounding_box()
        if box:
            self.page.mouse.move(
                box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=25
            )
            self.hold(0.3)

    def click(self, target: Locator, pause: float = 1.0) -> None:
        target = target.first
        target.wait_for(state="visible", timeout=30000)
        self.point(target)
        target.click()
        self.hold(pause)

    def type(self, target: Locator, text: str, delay: int = 45) -> None:
        self.click(target, pause=0.2)
        target.first.press_sequentially(text, delay=delay)

    def scroll(self, pixels: int, seconds: float = 2.0) -> None:
        steps = max(1, int(seconds * 20))
        for _ in range(steps):
            self.page.mouse.wheel(0, pixels / steps)
            self.hold(seconds / steps)

    def tab(self, name: str) -> None:
        self.click(
            self.page.get_by_role("tab", name=name, exact=True).or_(
                self.page.get_by_role("link", name=name, exact=True)
            ),
            pause=2.5,
        )

    def fast(self, label: str | None = None):
        demo = self

        class _Fast:
            def __enter__(self):
                demo.badge(label or f"▶▶ {demo.args.speedup:g}×")
                demo.rec.fast_start()

            def __exit__(self, *exc):
                demo.rec.fast_stop()
                demo.badge("")

        return _Fast()

    # --- Waldur API (off camera) --------------------------------------------

    def workspace(self) -> None:
        for ws in self.waldur.get(self.args.user, "/hpc-workspaces/"):
            if ws["account"] == self.args.allocation:
                self.resource_uuid = ws["resource_uuid"]
                return
        sys.exit(f"{self.args.user} has no HPC workspace for {self.args.allocation}")

    def job_state(self, job_id: str) -> str:
        job = self.waldur.get(
            self.args.user, f"/hpc-workspaces/{self.resource_uuid}/jobs/{job_id}/"
        )
        return (job.get("status") or {}).get("state") or job.get("state") or ""

    def wait_job(self, job_id: str, timeout: int = 240) -> str:
        deadline = time.time() + timeout
        state = ""
        while time.time() < deadline:
            state = self.job_state(job_id)
            if state in TERMINAL:
                return state
            self.hold(5)
        return state

    def prepare_failed_job(self) -> None:
        """A WRF run pointed at a case directory without input: fails for real."""
        spec = submit_workload.wrf(random.Random(7), nodes=2, fail=True)  # noqa: S311 (a fixed demo case)
        spec["name"] = "wrf-baltic-forecast"
        r = self.waldur.request(
            self.args.user,
            "POST",
            f"/hpc-workspaces/{self.resource_uuid}/jobs/",
            json={**spec, "environment": {}},
        )
        r.raise_for_status()
        self.failed_job = str(r.json()["job_id"])
        print(f"submitted failing job {self.failed_job}; waiting for it to end")

    # --- the assistant ------------------------------------------------------

    def assistant_open(self) -> None:
        page = self.page
        if not page.get_by_label("Message input").is_visible():
            self.click(page.get_by_test_id("llm-chat-drawer-toggle"), pause=1.5)
            understand = page.get_by_role("button", name="I understand")
            box = page.get_by_label("Message input")
            understand.or_(box).first.wait_for(timeout=30000)
            if understand.is_visible():
                self.click(understand, pause=1.0)
        expand = page.get_by_role("button", name="Expand to full screen")
        if expand.is_visible():
            self.click(expand, pause=1.0)
        self.assistant_hide_history()

    def assistant_new_chat(self) -> None:
        new_chat = self.page.get_by_role("button", name="New chat")
        if new_chat.count() and new_chat.first.is_visible():
            self.click(new_chat, pause=1.0)
        self.assistant_hide_history()

    def assistant_hide_history(self) -> None:
        # The history list would show earlier conversations: keep it closed.
        collapse = self.page.get_by_role("button", name="Collapse sidebar")
        if collapse.count() and collapse.first.is_visible():
            self.click(collapse, pause=0.8)

    def assistant_collapse(self) -> None:
        collapse = self.page.get_by_role("button", name="Collapse to panel")
        if collapse.is_visible():
            self.click(collapse, pause=1.0)

    def wait_answer(self, timeout: int = 300) -> None:
        stop = self.page.get_by_label("Stop generating").first
        with contextlib.suppress(Exception):  # answered before we looked
            stop.wait_for(state="visible", timeout=20000)
        stop.wait_for(state="hidden", timeout=timeout * 1000)
        self.hold(1.5)  # charts render after the stream ends

    def read_answer(self) -> None:
        self.page.evaluate(READ_ANSWER, [self.args.read_speed, 3, 25])
        self.hold(2.5)

    def last_answer(self) -> Locator:
        return self.page.locator(".aui-assistant-message-root").last

    def last_answer_text(self) -> str:
        answers = self.page.locator(".aui-assistant-message-root")
        return answers.last.inner_text() if answers.count() else ""

    def answer_form(self, prefer: list[str], texts: dict[str, str]) -> bool:
        """Fill the assistant's question form (ask_user), if it asked one.

        The questions differ from run to run. A choice question takes the
        option starting with one of `prefer`, else its first option; a text
        question takes the `texts` entry whose key appears in the question,
        else a request to choose a sensible default.
        """
        answer = self.last_answer()
        send = answer.locator(".aui-ask-user-submit")
        if not send.count():
            return False
        self.caption("The assistant asks what it needs to know", "Answered right in the chat")
        self.read_answer()
        questions = answer.locator(".aui-ask-user-question")
        for i in range(questions.count()):
            question = questions.nth(i)
            pills = question.locator(".aui-ask-user-pill")
            if pills.count():
                labels = [pills.nth(j).inner_text().strip() for j in range(pills.count())]
                pick = next(
                    (j for p in prefer for j, label in enumerate(labels) if label.startswith(p)), 0
                )
                if pills.nth(pick).get_attribute("aria-checked") != "true":
                    self.click(pills.nth(pick), pause=0.5)
                continue
            field = question.locator("input.aui-ask-user-text").first
            if field.count() and not field.input_value():
                asked = question.inner_text().lower()
                reply = next((v for k, v in texts.items() if k in asked), "any sensible default")
                self.type(field, reply, delay=35)
        self.click(send, pause=1)
        with self.fast():
            self.wait_answer()
        self.read_answer()
        return True

    def ask(
        self,
        prompt: str,
        read: bool = True,
        prefer: list[str] | None = None,
        texts: dict[str, str] | None = None,
    ) -> None:
        box = self.page.get_by_label("Message input")
        self.type(box, prompt, delay=18)
        self.hold(0.6)
        box.press("Enter")
        self.hold(1.0)
        with self.fast():
            self.wait_answer()
        if read:
            self.read_answer()
        for _ in range(2):
            if not self.answer_form(prefer or [], texts or {}):
                break

    def submit_from_chat(self) -> str:
        """Press Submit on the newest job card (if the assistant left one); the job id."""
        submit = self.last_answer().get_by_role("button", name="Submit", exact=True)
        if submit.count():
            self.caption("Submit from the chat")
            self.click(submit, pause=1)
            with self.fast():
                self.wait_answer()
            self.read_answer()
        text = self.last_answer_text()
        found = re.findall(r"Job ID:?\s*(\d+)", text) or re.findall(
            r"\bjob (?:ID )?(\d{2,})", text, re.IGNORECASE
        )
        return found[-1] if found else ""

    def watch_job(self, job_id: str) -> str:
        """Close the chat and follow the job in the Jobs table until it ends."""
        self.caption(
            "The job runs on the cluster", "Pending, running, completed: the table follows it live"
        )
        self.assistant_collapse()
        self.click(self.page.get_by_test_id("llm-chat-drawer-toggle"), pause=1)  # close the panel
        self.tab("Jobs")
        with self.fast():
            state = self.wait_job(job_id) if job_id else ""
            if not job_id:
                self.hold(60)
            self.hold(6)  # the table's next poll
        self.hold(3)
        print(f"job {job_id or '?'}: {state or 'unknown'}")
        return state

    # --- scenes -------------------------------------------------------------

    def scene_login(self) -> None:
        page = self.page
        page.goto(f"{self.url}/login/")
        page.wait_for_load_state("networkidle")
        self.caption(
            "Waldur HPC workspace", "Slurm jobs, files and an AI assistant, for an HPC allocation"
        )
        self.hold(3)
        self.caption(
            "Sign in with the HPC centre's identity provider",
            "Keycloak; Waldur calls FireCREST with the user's own token",
        )
        self.click(page.get_by_text("HPC centre", exact=False), pause=0)
        page.wait_for_url("**/auth/realms/**", timeout=30000)
        page.wait_for_load_state("networkidle")
        self.caption(
            "Sign in with the HPC centre's identity provider",
            "Keycloak; Waldur calls FireCREST with the user's own token",
        )
        self.type(page.locator("#username"), self.args.user, delay=90)
        self.type(page.locator("#password"), self.args.password, delay=90)
        self.click(page.locator("#kc-login"), pause=0)
        page.wait_for_url(f"{self.url}/**", timeout=60000)
        page.wait_for_load_state("networkidle")
        self.caption()
        self.hold(1.5)

    def open_allocation(self, tab: str = "hpc-jobs") -> None:
        self.page.goto(f"{self.url}/resource-details/{self.resource_uuid}/?tab={tab}")
        self.page.wait_for_load_state("networkidle")
        self.hold(2)

    def scene_overview(self) -> None:
        self.open_allocation()
        self.caption(
            "An allocation = a Slurm account on the cluster",
            "CPU-hour and GPU-hour limits, maintenance announced by the centre",
        )
        self.point(self.page.get_by_text("Climate allocation", exact=True).first)
        self.hold(4)
        self.caption(
            "Jobs of the allocation, live from Slurm through FireCREST",
            "Only this allocation's account: other projects' jobs never show",
        )
        self.tab("Jobs")
        self.hold(4)
        row = (
            self.page.get_by_role("row").filter(has_text=self.failed_job).first
            if self.failed_job
            else None
        )
        if row is not None and row.count():
            self.point(row)
            self.hold(2)

    def scene_insights(self) -> None:
        self.open_allocation("hpc-jobs")
        self.caption(
            "Allocation burn-down and run-out forecast",
            "At the recent pace the CPU-hours run out before the allocation ends",
        )
        self.tab("Insights")
        self.hold(5)
        self.scroll(500, 2.5)
        self.hold(3)
        self.scroll(-500, 1.5)
        for sub, text in [
            ("Efficiency", "Requested vs used time, CPU and memory efficiency"),
            ("Queue", "Queue wait per partition"),
            ("Cluster load", "Node load across the cluster: drained and down nodes in red"),
        ]:
            self.caption(f"Insights: {sub.lower()}", text)
            self.tab(sub)
            self.hold(4)

    def scene_cluster(self) -> None:
        self.open_allocation("hpc-jobs")
        self.caption(
            "Cluster: health, partitions and their limits",
            "Read from the cluster; what the assistant validates jobs against",
        )
        self.tab("Cluster")
        self.hold(3)
        self.scroll(450, 2)
        self.hold(3)
        self.scroll(-450, 1)
        self.caption(
            "Software: the EESSI catalog, per architecture",
            "x86_64 zen3 on batch/debug, aarch64 + NVIDIA on gpu",
        )
        self.tab("Software")
        self.hold(2)
        gromacs = self.page.get_by_role("row").filter(has_text="Molecular dynamics").first
        if gromacs.count():
            self.click(gromacs.get_by_role("button").first, pause=3)
            self.scroll(350, 2)
        self.hold(3)

    def open_file(self, name: str, action: str) -> None:
        row = self.page.get_by_role("row").filter(has=self.page.get_by_text(name, exact=True)).first
        self.click(row.get_by_role("button", name="Actions"), pause=0.6)
        self.click(self.page.get_by_role("menuitem", name=action), pause=3)

    def close_dialog(self) -> None:
        self.page.keyboard.press("Escape")
        self.hold(1)

    def into(self, folder: str) -> None:
        self.click(self.page.get_by_text(folder, exact=True), pause=2)

    def up_home(self) -> None:
        # The Files breadcrumb (home / alice / …), not the header's user name.
        crumb = self.page.get_by_text("home", exact=True).locator(
            f"xpath=following::*[normalize-space()='{self.args.user}'][1]"
        )
        self.click(crumb, pause=2)

    def scene_files(self) -> None:
        self.open_allocation("hpc-jobs")
        self.caption(
            "Files: the home directory on the cluster", "Browsed over FireCREST's filesystem API"
        )
        self.tab("Files")
        self.hold(3)
        self.caption(
            "A GROMACS energy file, plotted",
            ".xvg, CSV/TSV and WRF logs get a Plot view; the backend parses them",
        )
        self.into("md")
        self.open_file("energy.xvg", "Plot")
        self.hold(5)
        self.click(self.page.get_by_role("tab", name="Content"), pause=3)
        self.close_dialog()
        self.up_home()
        self.caption(
            "A WRF run's log: forecast progress and time left",
            "rsl.error.0000 plotted from the running model's timings",
        )
        self.into("wrf-baltic")
        self.open_file("rsl.error.0000", "Plot")
        self.hold(5)
        self.close_dialog()
        self.up_home()
        self.caption(
            "An HTML report, rendered",
            "MultiQC in a sandboxed frame: its scripts cannot reach the portal",
        )
        self.into("qc")
        self.open_file("multiqc_report.html", "View")
        self.hold(6)
        self.close_dialog()
        self.up_home()

    def scene_performance(self) -> None:
        self.open_allocation("hpc-jobs")
        self.caption(
            "A job's performance, sampled on the nodes",
            "Sonar -> slurm-monitor: CPU, memory, GPU use and processes over time",
        )
        self.tab("Jobs")
        # A finished GPU job with telemetry: md-equil from the seeded history.
        self.click(self.page.get_by_role("button", name="Clear filters"), pause=2)
        row = self.page.get_by_role("row").filter(has_text="md-equil").first
        self.click(row.get_by_role("button").first, pause=3)
        perf = self.page.get_by_role("tab", name="Performance")
        if perf.count():
            self.click(perf, pause=4)
        self.scroll(600, 3)
        self.hold(4)
        self.scroll(-600, 1.5)

    def scene_submit_form(self) -> None:
        page = self.page
        self.open_allocation("hpc-jobs")
        self.caption(
            "Submit a job from the portal",
            "Software from the catalog, limits checked as you type, the batch script generated",
        )
        self.tab("Jobs")
        self.click(page.get_by_role("button", name="Submit job"), pause=2)
        dialog = page.get_by_role("dialog")
        dialog.get_by_label("Job name").first.fill("")
        self.type(dialog.get_by_label("Job name"), "wrf-nordic-9km", delay=60)
        self.hold(1)
        # Replace the template's commands with the run itself.
        editor_line = dialog.get_by_text("srun", exact=False).last
        if editor_line.count():
            self.click(editor_line, pause=0.3)
            page.keyboard.press("ControlOrMeta+a")
            page.keyboard.press("Delete")
            page.keyboard.type("srun wrf.exe", delay=70)
            self.hold(1)
        # Partition, QoS, software: the software picker is the last one.
        self.caption(
            "Software from the cluster's catalog", "Picking it adds the module lines to the script"
        )
        self.click(dialog.get_by_role("combobox").last, pause=0.8)
        page.keyboard.type("WRF", delay=120)
        self.hold(2.5)
        page.keyboard.press("Enter")
        self.hold(2.5)
        self.caption(
            "Too big for the partition? Said before you submit",
            "batch allows 16 nodes and 48 hours",
        )
        nodes = dialog.get_by_role("spinbutton").first
        nodes.fill("")
        self.type(nodes, "40", delay=150)
        self.hold(4)
        nodes.fill("")
        self.type(nodes, "8", delay=150)
        self.hold(3)
        self.caption()
        self.click(dialog.get_by_role("button", name="Cancel"), pause=1)
        discard = page.get_by_role("button", name="Discard and exit")
        if discard.count() and discard.is_visible():
            self.click(discard)

    def scene_ai_usage(self) -> None:
        self.open_allocation("hpc-jobs")
        self.caption(
            "The AI assistant knows your allocations",
            "It calls Waldur's HPC tools with your permissions",
        )
        self.assistant_open()
        self.assistant_new_chat()
        self.caption("Calculation: how long does the allocation last?")
        self.ask(
            "How much of my Climate allocation is left, and when will it run out at the current pace?"
        )

    def scene_ai_plan(self) -> None:
        self.assistant_open()
        self.caption(
            "Calculation: what will a job cost?",
            "Planned against the partition limits; nothing runs until you confirm",
        )
        self.ask(
            "I want to run GROMACS on 2 nodes in the batch partition for 4 hours in my Climate "
            "allocation. How many CPU-hours will that use? Prepare the job but do not submit it yet.",
            prefer=["128", "2024", "gmx mdrun -deffnm", "256"],
            texts={
                "director": "/home/alice/md",
                "path": "/home/alice/md",
                "command": "gmx mdrun -deffnm md",
                "input": "md.tpr in /home/alice/md",
                "memory": "default",
            },
        )

    def scene_ai_program(self) -> None:
        self.assistant_open()
        self.caption(
            "Write a program and run it on the cluster",
            "The assistant writes the code and the batch script",
        )
        self.ask(
            "Write a Python program that multiplies two random 400x400 matrices using only the "
            "standard library, splitting the rows across all CPU cores of the node with "
            "multiprocessing, and prints the elapsed time and a checksum of the result. Prepare it "
            "as a job on 1 node in the debug partition of my Climate allocation with a 10 minute "
            "limit, and show it to me before submitting."
        )
        # A program written in one go does not always run: if it fails, the
        # assistant reads the error, fixes it and the fixed one is submitted.
        for attempt in range(3):
            self.program_job = self.submit_from_chat()
            state = self.watch_job(self.program_job)
            self.assistant_open()
            if state != "FAILED" or attempt == 2:
                break
            self.caption(
                "The program failed: debug it with the assistant",
                "It reads the error output, fixes the code and prepares the job again",
            )
            self.ask(
                f"Job {self.program_job} failed. Read its error output, fix the program and "
                "prepare the corrected job for me to submit."
            )
        self.caption("The real output, explained")
        self.ask("Show me the output of that matrix job and explain the result briefly.")

    def scene_ai_debug(self) -> None:
        self.assistant_open()
        self.caption(
            "Debugging: why did my job fail?",
            "It reads the job's state, exit code and error output",
        )
        self.ask(
            f"My job {self.failed_job} in the Climate allocation failed. Why, and how do I fix it?"
        )

    def scene_ai_rightsize(self) -> None:
        self.assistant_open()
        self.caption(
            "Debugging: am I asking for too much?",
            "Requested vs used time and efficiency over the last week",
        )
        self.ask(
            "Looking at my Climate allocation's jobs from the last week: which ones request far "
            "more time than they use? Suggest how to right-size them."
        )

    def scene_ai_maintenance(self) -> None:
        self.assistant_open()
        self.caption(
            "Planning around maintenance",
            "Reservations on the cluster and the centre's announcements",
        )
        self.ask(
            "If I submit a 48-hour WRF job on 8 nodes in the batch partition of my Climate "
            "allocation now, will it finish before the planned maintenance? If not, what should I do?"
        )

    def scene_ai_software(self) -> None:
        self.assistant_open()
        self.caption(
            "Which software can I use where?", "The EESSI catalog, per partition architecture"
        )
        self.ask(
            "Which GROMACS builds can I use on the gpu partition of my Climate allocation, and how do I load one?"
        )

    def scene_ai_plot(self) -> None:
        self.assistant_open()
        self.caption("Look at results without leaving the chat")
        self.ask("Plot /home/alice/md/energy.xvg from my Climate allocation.")

    def scene_outro(self) -> None:
        self.caption(
            "Waldur HPC workspace",
            "Jobs, files, insights and an assistant that calculates, runs and debugs",
        )
        self.hold(5)
        self.caption()
        self.hold(1)


SCENES = [
    "overview",
    "insights",
    "cluster",
    "files",
    "performance",
    "submit-form",
    "ai-usage",
    "ai-plan",
    "ai-program",
    "ai-debug",
    "ai-rightsize",
    "ai-maintenance",
    "ai-software",
    "ai-plot",
    "outro",
]


def assemble(rec: Recorder, out: Path, speedup: float, fps: int, end: float) -> None:
    if not rec.frames:
        sys.exit("no frames recorded")
    listing = rec.frames_dir / "frames.txt"
    lines = []
    for i, (t, path) in enumerate(rec.frames):
        nxt = rec.frames[i + 1][0] if i + 1 < len(rec.frames) else end
        duration = max(0.0, nxt - t)
        if rec.is_fast(t):
            duration /= speedup
        lines.append(f"file '{path.name}'\nduration {duration:.4f}")
    lines.append(f"file '{rec.frames[-1][1].name}'")  # concat demuxer: last entry repeated
    listing.write_text("\n".join(lines) + "\n")
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-loglevel",
            "error",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(listing),
            "-vf",
            f"fps={fps},scale={WIDTH}:{HEIGHT}:flags=lanczos,format=yuv420p",
            "-c:v",
            "libx264",
            "-preset",
            "slow",
            "-crf",
            "18",
            "-movflags",
            "+faststart",
            str(out),
        ],
        check=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--homeport-url", default="http://localhost:8001")
    parser.add_argument(
        "--waldur-url", default="http://localhost:8000", help="mastermind, for the off-camera setup"
    )
    parser.add_argument("--user", default="alice")
    parser.add_argument("--password", default="password")
    parser.add_argument(
        "--allocation", default="alloc_climate", help="Slurm account of the allocation shown"
    )
    parser.add_argument(
        "--scenes",
        default=",".join(SCENES),
        help=f"comma-separated, in any order of: {', '.join(SCENES)}",
    )
    parser.add_argument("--out", default="hpc-demo.mp4")
    parser.add_argument(
        "--speedup",
        type=float,
        default=6.0,
        help="playback speed while waiting on the model or a job",
    )
    parser.add_argument(
        "--read-speed", type=float, default=70.0, help="answer scroll speed, pixels per second"
    )
    parser.add_argument("--fps", type=int, default=30)
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--keep-frames", action="store_true")
    args = parser.parse_args()
    sys.stdout.reconfigure(line_buffering=True)  # progress shows in a log as it happens

    wanted = [s.strip() for s in args.scenes.split(",") if s.strip()]
    unknown = sorted(set(wanted) - set(SCENES))
    if unknown:
        parser.error(f"unknown scenes: {', '.join(unknown)}")
    if not shutil.which("ffmpeg"):
        parser.error("ffmpeg is needed to assemble the video")

    frames_dir = Path(tempfile.mkdtemp(prefix="hpc-demo-frames-"))
    rec = Recorder(frames_dir)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headed)
        context = browser.new_context(
            viewport={"width": WIDTH, "height": HEIGHT}, device_scale_factor=1
        )
        context.add_init_script(OVERLAY)
        page = context.new_page()
        demo = Demo(args, page, rec)
        demo.workspace()
        if "ai-debug" in wanted:
            demo.prepare_failed_job()
            print(f"failing job {demo.failed_job}: {demo.wait_job(demo.failed_job, timeout=180)}")

        cdp = context.new_cdp_session(page)
        cdp.on("Page.screencastFrame", lambda params: rec.on_frame(cdp, params))
        cdp.send(
            "Page.startScreencast",
            {"format": "jpeg", "quality": 90, "maxWidth": WIDTH, "maxHeight": HEIGHT},
        )
        try:
            demo.scene_login()
            for name in wanted:
                print(f"scene {name}")
                getattr(demo, "scene_" + name.replace("-", "_"))()
        except Exception:
            page.screenshot(path=Path(args.out).with_suffix(".error.png"))
            Path(args.out).with_suffix(".error.html").write_text(page.content())
            raise
        finally:
            end = time.time()
            cdp.send("Page.stopScreencast")
            browser.close()
            print(f"{len(rec.frames)} frames; assembling {args.out}")
            assemble(rec, Path(args.out), args.speedup, args.fps, end)
            if args.keep_frames:
                print(f"frames kept in {frames_dir}")
            else:
                shutil.rmtree(frames_dir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
