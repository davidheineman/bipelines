import os
import re
import subprocess
import time
from typing import Optional, Tuple

from beaker import Beaker, BeakerWorkloadStatus
from rich.console import Console

console = Console()

EXPERIMENT_RE = re.compile(r"Experiment:\s+(\S+)\s+→\s+(https://beaker\.org/ex/(\S+))")


def parse_experiment_line(line: str) -> Optional[Tuple[str, str, str]]:
    """Parse 'Experiment: name → url' returning (name, url, experiment_id), or None."""
    m = EXPERIMENT_RE.search(line)
    if m:
        return m.group(1), m.group(2), m.group(3)
    return None


def run_command_and_capture_experiment(
    command: str,
    env: Optional[dict] = None,
    cwd: Optional[str] = None,
) -> Tuple[str, str, str]:
    """Run a command locally, streaming output and capturing the experiment line.

    Returns (experiment_name, url, experiment_id).
    Raises RuntimeError if the command fails or no experiment line is found.
    """
    merged_env = {**os.environ, **(env or {})}
    merged_env.setdefault("COLUMNS", "500")

    proc = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=merged_env,
        cwd=cwd,
    )

    experiment_info = None
    for line in proc.stdout:
        stripped = line.rstrip("\n")
        console.print(f"  [dim]{stripped}[/dim]")
        if experiment_info is None:
            parsed = parse_experiment_line(stripped)
            if parsed:
                experiment_info = parsed

    proc.wait()

    if proc.returncode != 0:
        raise RuntimeError(f"Command exited with code {proc.returncode}")

    if experiment_info is None:
        raise RuntimeError("No 'Experiment: ... → ...' line found in command output")

    return experiment_info


def get_experiment_status(beaker: Beaker, experiment_id: str) -> str:
    """Get the current status of a Beaker experiment by ID."""
    workload = beaker.workload.get(experiment_id)
    job = beaker.workload.get_latest_job(workload)

    if job is None:
        return "pending"

    STATUS_MAP = {
        BeakerWorkloadStatus.running: "running",
        BeakerWorkloadStatus.succeeded: "completed",
        BeakerWorkloadStatus.failed: "failed",
        BeakerWorkloadStatus.canceled: "canceled",
    }
    return STATUS_MAP.get(job.status.status, "unknown")


def wait_for_experiment(
    beaker: Beaker,
    experiment_id: str,
    poll_interval: float = 15.0,
) -> str:
    """Poll a Beaker experiment until it reaches a terminal state. Returns final status."""
    last_status = None

    while True:
        status = get_experiment_status(beaker, experiment_id)

        if status != last_status:
            console.print(f"  Status: [yellow]{status}[/yellow]")
            last_status = status

        if status in ("completed", "failed", "canceled"):
            return status

        time.sleep(poll_interval)
