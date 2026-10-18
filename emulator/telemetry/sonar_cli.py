"""``slurm-sonar-emulator``: publish synthetic Sonar telemetry to Kafka.

Reads the emulator's JSON state (the same file the slurm container writes;
read-only is enough) and publishes what Sonar daemons on the emulated nodes
would: see ``emulator/telemetry/sonar.py`` for the message builders.

Each tick (every ``--interval`` seconds):

1. **Catch-up / backfill**: every finished job (accounting row with start,
   end and nodes) that ended within ``--backfill-days`` and has not been
   published yet gets samples over its lifetime on each of its nodes
   (``--spacing`` apart, finer for short jobs) and its final job record.
   Published job ids are kept in ``<state-dir>/sonar-emulator.json``, so a
   restart does not publish them again (``--reset`` forgets them). The first
   tick is the backfill; later ticks pick up jobs as they finish, skipping
   the instants the live loop already sampled.
2. **Live**: one sample per node (DOWN nodes stay silent, as an unreachable
   probe would), with the processes of the running jobs; job records of the
   running jobs; partitions and node states.
3. **Sysinfo** for every node every 6 hours, on the first tick back to the
   start of the backfill window (see ``publish_sysinfo`` for why 6 h).

Timestamps are the wall clock, which the demo stack's emulator follows
(``SLURM_EMULATOR_TIME_FOLLOW_WALL=1``).
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional, Protocol, TextIO

from emulator.core import cluster_model, job_execution
from emulator.core.database import SlurmDatabase
from emulator.core.scheduler import advance_job_states, expected_end
from emulator.core.time_engine import TimeEngine
from emulator.telemetry import sonar

STATE_FILE_NAME = "sonar-emulator.json"
SYSINFO_INTERVAL = timedelta(hours=6)


class Sink(Protocol):
    def send(self, topic: str, message: dict[str, Any]) -> None: ...

    def flush(self) -> None: ...


class StreamSink:
    """Tab-separated topic + JSON lines, for ``--output`` and tests."""

    def __init__(self, stream: TextIO) -> None:
        self.stream = stream
        self.count = 0

    def send(self, topic: str, message: dict[str, Any]) -> None:
        self.stream.write(f"{topic}\t{json.dumps(message, separators=(',', ':'))}\n")
        self.count += 1

    def flush(self) -> None:
        self.stream.flush()


class KafkaSink:
    def __init__(self, bootstrap: str, retry_seconds: float = 5.0) -> None:
        from kafka import KafkaProducer  # noqa: PLC0415 - optional extra
        from kafka.errors import NoBrokersAvailable  # noqa: PLC0415

        while True:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=bootstrap,
                    value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode(),
                    linger_ms=50,
                    max_request_size=8 * 1024 * 1024,
                )
                break
            except NoBrokersAvailable:
                print(f"no Kafka broker at {bootstrap} yet, retrying", file=sys.stderr)
                time.sleep(retry_seconds)
        self.count = 0

    def send(self, topic: str, message: dict[str, Any]) -> None:
        self.producer.send(topic, message)
        self.count += 1

    def flush(self) -> None:
        self.producer.flush()


@dataclass
class PublisherState:
    """What has been published, persisted across restarts."""

    emitted: set[int] = field(default_factory=set)
    # job id -> [first, last] instant the live loop sampled it (ISO)
    live: dict[str, list[str]] = field(default_factory=dict)
    sysinfo_at: Optional[str] = None

    @classmethod
    def load(cls, path: Path) -> PublisherState:
        try:
            data = json.loads(path.read_text())
        except (OSError, ValueError):
            return cls()
        return cls(
            emitted={int(x) for x in data.get("emitted", [])},
            live=dict(data.get("live", {})),
            sysinfo_at=data.get("sysinfo_at"),
        )

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(
                {"emitted": sorted(self.emitted), "live": self.live, "sysinfo_at": self.sysinfo_at}
            )
        )
        tmp.replace(path)


@dataclass
class Snapshot:
    finished: list[sonar.JobRun]
    running: list[sonar.JobRun]
    node_states: dict[str, list[str]]
    background: set[str]


def read_snapshot(database: SlurmDatabase, now: datetime) -> Snapshot:
    """Jobs, node states and background load at ``now`` (aware UTC)."""
    local_now = now.astimezone().replace(tzinfo=None)  # the emulator's naive clock
    finished = [r for r in map(sonar.run_from_record, database.usage_records) if r is not None]
    running: list[sonar.JobRun] = []
    for job in database.jobs.values():
        if job.state != "RUNNING":
            continue
        end = expected_end(job)
        if end is not None and end <= local_now:  # the controller has not advanced it yet
            continue
        run = sonar.run_from_job(job)
        if run is not None:
            running.append(run)
    finished_ids = {r.job_id for r in finished}
    running = [r for r in running if r.job_id not in finished_ids]
    views = cluster_model.node_views(database, local_now)
    usage = cluster_model.occupancy(database, local_now)
    return Snapshot(
        finished=finished,
        running=running,
        node_states={node: view.state for node, view in views.items()},
        background={node for node, use in usage.items() if use.background},
    )


def _runs_by_node(runs: list[sonar.JobRun]) -> dict[str, list[sonar.JobRun]]:
    out: dict[str, list[sonar.JobRun]] = {}
    for run in runs:
        for node in run.nodes:
            out.setdefault(node, []).append(run)
    return out


def publish_catch_up(
    sink: Sink,
    state: PublisherState,
    snapshot: Snapshot,
    cluster: str,
    now: datetime,
    backfill_days: float,
    spacing: float,
) -> int:
    """Lifetime samples + final record of finished jobs not published yet."""
    window_start = now - timedelta(days=backfill_days)
    by_node = _runs_by_node(snapshot.finished + snapshot.running)
    jobs = 0
    for run in sorted(snapshot.finished, key=lambda r: r.end_time or now):
        if run.job_id in state.emitted:
            continue
        if run.end_time is None or run.end_time < window_start:
            continue
        seen = state.live.get(str(run.job_id))
        first = datetime.fromisoformat(seen[0]) if seen else None
        last = datetime.fromisoformat(seen[1]) if seen else None
        for at in sonar.sample_times(run, now, spacing):
            if at < window_start or (first and last and first <= at <= last):
                continue
            for node in run.nodes:
                sink.send(
                    sonar.topic(cluster, "sample"),
                    sonar.sample_message(cluster, node, at, by_node.get(node, [])),
                )
        sink.send(sonar.topic(cluster, "job"), sonar.job_message(cluster, run.end_time, [run]))
        state.emitted.add(run.job_id)
        state.live.pop(str(run.job_id), None)
        jobs += 1
    return jobs


def publish_live(
    sink: Sink, state: PublisherState, snapshot: Snapshot, cluster: str, now: datetime
) -> None:
    by_node = _runs_by_node(snapshot.running)
    for node in cluster_model.all_nodes():
        if "DOWN" in snapshot.node_states.get(node, []):
            continue
        sink.send(
            sonar.topic(cluster, "sample"),
            sonar.sample_message(
                cluster, node, now, by_node.get(node, []), background=node in snapshot.background
            ),
        )
    stamp = sonar.iso(now)
    for run in snapshot.running:
        seen = state.live.setdefault(str(run.job_id), [stamp, stamp])
        seen[1] = stamp
    if snapshot.running:
        sink.send(sonar.topic(cluster, "job"), sonar.job_message(cluster, now, snapshot.running))
    sink.send(
        sonar.topic(cluster, "cluster"), sonar.cluster_message(cluster, now, snapshot.node_states)
    )


def publish_sysinfo(
    sink: Sink,
    state: PublisherState,
    cluster: str,
    now: datetime,
    backfill_days: float,
    pause: float = 0.0,
) -> bool:
    """Every node's sysinfo, every ``SYSINFO_INTERVAL`` (back to the window start).

    slurm-monitor scales a node's memory series by the newest sysinfo no more
    than 12 h before the query's end (``get_node_memory``) and returns no CPU
    or memory series without one, so finished jobs need sysinfo dated within
    12 h before their end: hence a sysinfo every 6 h over the backfill window.

    Each round (one message per node) is followed by ``pause`` seconds: the
    listener upserts a whole poll (up to 100 messages) in one statement, and
    two sysinfo messages of the same node in one poll make that statement
    fail ("ON CONFLICT DO UPDATE command cannot affect row a second time"),
    after which it falls back to row-by-row writes that deadlock with the
    sample topic's inserts. Rounds a poll apart keep each poll duplicate-free.
    """
    last = datetime.fromisoformat(state.sysinfo_at) if state.sysinfo_at else None
    if last is not None and now - last < SYSINFO_INTERVAL:
        return False
    stamps = [now]
    if last is None:
        at = now - timedelta(days=backfill_days)
        stamps = []
        while at < now:
            stamps.append(at)
            at += SYSINFO_INTERVAL
        stamps.append(now)
    for at in stamps:
        for node in cluster_model.all_nodes():
            sink.send(sonar.topic(cluster, "sysinfo"), sonar.sysinfo_message(cluster, node, at))
        sink.flush()
        if pause:
            time.sleep(pause)
    state.sysinfo_at = sonar.iso(now)
    return True


def tick(
    sink: Sink,
    state: PublisherState,
    database: SlurmDatabase,
    cluster: str,
    now: datetime,
    backfill_days: float,
    spacing: float,
    pause: float = 0.0,
) -> dict[str, int]:
    """One publishing round; ``pause`` paces sysinfo (see ``publish_sysinfo``)."""
    snapshot = read_snapshot(database, now)
    publish_sysinfo(sink, state, cluster, now, backfill_days, pause)
    jobs = publish_catch_up(sink, state, snapshot, cluster, now, backfill_days, spacing)
    publish_live(sink, state, snapshot, cluster, now)
    sink.flush()
    return {"caught_up_jobs": jobs, "running_jobs": len(snapshot.running)}


def _parser() -> argparse.ArgumentParser:
    env = os.environ.get
    parser = argparse.ArgumentParser(
        prog="slurm-sonar-emulator",
        description="Publish synthetic Sonar telemetry for the emulated cluster to Kafka.",
    )
    parser.add_argument("--cluster", default=env("SONAR_CLUSTER", "hpc-demo"))
    parser.add_argument(
        "--kafka", default=env("SONAR_KAFKA", "localhost:9092"), help="bootstrap host:port"
    )
    parser.add_argument(
        "--backfill-days",
        type=float,
        default=float(env("SONAR_BACKFILL_DAYS", "7")),
        help="publish finished jobs that ended within this many days (once per job)",
    )
    parser.add_argument("--interval", type=float, default=float(env("SONAR_INTERVAL", "60")))
    parser.add_argument(
        "--spacing", type=float, default=300.0, help="seconds between a finished job's samples"
    )
    parser.add_argument(
        "--state-dir",
        default=env("SONAR_STATE_DIR", "/var/lib/sonar-emulator"),
        help="where published job ids are remembered",
    )
    parser.add_argument(
        "--pause",
        type=float,
        default=3.0,
        help="seconds between sysinfo rounds, longer than the listener's poll (2 s)",
    )
    parser.add_argument("--once", action="store_true", help="run one tick and exit")
    parser.add_argument("--reset", action="store_true", help="forget what was published")
    parser.add_argument(
        "--output", help="write '<topic>\\t<json>' lines here ('-' = stdout) instead of Kafka"
    )
    return parser


def _catch_up_running(database: SlurmDatabase) -> None:
    """Take the running jobs from a caught-up copy of the state.

    The emulator advances jobs only when something reads them, but a probe
    on the node sees a running job whether or not anyone polls the
    controller. The copy is advanced in memory without running batch
    scripts and is never saved; finished jobs still come from the saved
    accounting, so their exit codes are the ones the controller recorded.
    """
    live = copy.deepcopy(database)
    saved = os.environ.get(job_execution.EXECUTE_ENV)
    os.environ[job_execution.EXECUTE_ENV] = "0"
    try:
        advance_job_states(live, TimeEngine())
    finally:
        if saved is None:
            os.environ.pop(job_execution.EXECUTE_ENV, None)
        else:
            os.environ[job_execution.EXECUTE_ENV] = saved
    database.jobs = live.jobs


def main(argv: Optional[list[str]] = None) -> None:
    args = _parser().parse_args(argv)
    state_path = Path(args.state_dir) / STATE_FILE_NAME
    state = PublisherState() if args.reset else PublisherState.load(state_path)
    if args.output:
        sink: Sink = StreamSink(sys.stdout if args.output == "-" else Path(args.output).open("a"))  # noqa: SIM115
    else:
        sink = KafkaSink(args.kafka)
    database = SlurmDatabase()
    while True:
        started = time.monotonic()
        database.load_state()
        _catch_up_running(database)
        now = datetime.now(timezone.utc).replace(microsecond=0)
        stats = tick(
            sink, state, database, args.cluster, now, args.backfill_days, args.spacing, args.pause
        )
        state.save(state_path)
        print(
            f"{sonar.iso(now)} {args.cluster}: {stats['caught_up_jobs']} finished job(s) published,"
            f" {stats['running_jobs']} running, {getattr(sink, 'count', 0)} messages so far",
            file=sys.stderr,
            flush=True,
        )
        if args.once:
            return
        time.sleep(max(args.interval - (time.monotonic() - started), 1.0))


if __name__ == "__main__":
    main()
