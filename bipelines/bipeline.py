import json
import re
import time
from pathlib import Path
from typing import Optional

from beaker import Beaker
from beaker import beaker_pb2 as pb2
from rich.console import Console
from rich.table import Table

from bipelines.config import CommandConfig, BipelineConfig
from bipelines.experiment import (
    get_experiment_status,
    run_command_and_capture_experiment,
)
from bipelines.local_env import setup_local_env

console = Console()

HASH_TAG_RE = re.compile(r"\(bipelines:([a-f0-9]+)\)\s*")
HASH_TAG_SEARCH = "(bipelines:"

WORKLOAD_STATUS_DISPLAY = {
    pb2.WorkloadStatus.STATUS_SUBMITTED: "pending",
    pb2.WorkloadStatus.STATUS_QUEUED: "pending",
    pb2.WorkloadStatus.STATUS_INITIALIZING: "pending",
    pb2.WorkloadStatus.STATUS_READY_TO_START: "pending",
    pb2.WorkloadStatus.STATUS_RUNNING: "running",
    pb2.WorkloadStatus.STATUS_STOPPING: "running",
    pb2.WorkloadStatus.STATUS_UPLOADING_RESULTS: "running",
    pb2.WorkloadStatus.STATUS_SUCCEEDED: "completed",
    pb2.WorkloadStatus.STATUS_FAILED: "failed",
    pb2.WorkloadStatus.STATUS_CANCELED: "canceled",
}


def _parse_hash_tag(description: str) -> Optional[str]:
    """Extract the bipelines task hash from a description like '(bipelines:abc123) ...'."""
    m = HASH_TAG_RE.match(description)
    return m.group(1) if m else None


class Bipeline:
    def __init__(self, config: BipelineConfig):
        self.config = config
        self.beaker = Beaker.from_env()
        self._workload_cache: dict[str, pb2.Workload] = {}

    # ── Beaker-based deduplication ──────────────────────────────────────

    def _build_workload_cache(self):
        """Pre-fetch all bipelines-tagged workloads from the Beaker workspace."""
        self._workload_cache = {}
        if not self.config.workspace:
            return
        try:
            for w in self.beaker.workload.list(
                workspace=self.config.workspace,
                name_or_description=HASH_TAG_SEARCH,
            ):
                task_hash = _parse_hash_tag(w.experiment.description or "")
                if task_hash and task_hash not in self._workload_cache:
                    self._workload_cache[task_hash] = w
        except Exception as e:
            console.print(f"  [dim]Warning: could not query Beaker workspace: {e}[/dim]")

    def _tag_experiment(self, experiment_id: str, task_hash: str):
        """Prepend the bipelines hash tag to the experiment description, preserving any original text.

        Idempotent: strips an existing tag before re-applying, so the job's own
        description updates are kept intact even if we re-tag periodically.
        """
        try:
            workload = self.beaker.workload.get(experiment_id)
            current_desc = workload.experiment.description or ""
            original = HASH_TAG_RE.sub("", current_desc, count=1)
            new_desc = f"(bipelines:{task_hash}) {original}".rstrip()
            if new_desc != current_desc:
                self.beaker.workload.update(workload, description=new_desc)
        except Exception as e:
            console.print(f"  [dim]Warning: could not tag experiment: {e}[/dim]")

    def _wait_for_experiment(
        self,
        experiment_id: str,
        task_hash: str,
        poll_interval: float = 15.0,
        retag_every: int = 4,
    ) -> str:
        """Poll a Beaker experiment until terminal, re-tagging the description periodically."""
        last_status = None
        polls = 0

        while True:
            status = get_experiment_status(self.beaker, experiment_id)

            if status != last_status:
                console.print(f"  Status: [yellow]{status}[/yellow]")
                last_status = status

            if status in ("completed", "failed", "canceled"):
                self._tag_experiment(experiment_id, task_hash)
                return status

            polls += 1
            if polls % retag_every == 0:
                self._tag_experiment(experiment_id, task_hash)

            time.sleep(poll_interval)

    # ── Main loop ──────────────────────────────────────────────────────

    def run(self) -> list[dict]:
        """Execute all tasks and return a list of result dicts.

        Each dict has keys: command, hash, status.
        """
        cfg = self.config

        console.print()
        console.print("[bold]Bipelines[/bold]")
        console.print(f"  Run hash:   {cfg.run_hash or '(none)'}")
        console.print(f"  Workspace:  {cfg.workspace or '(none — dedup disabled)'}")
        console.print(f"  Commands:   {len(cfg.commands)}")
        if cfg.repos:
            console.print(f"  Repos:      {len(cfg.repos)} (local install)")
        if cfg.dry_run:
            console.print("  [yellow]DRY RUN — commands will not be executed[/yellow]")
        console.print()

        if cfg.workspace:
            console.print("[dim]Fetching existing experiments from Beaker...[/dim]")
            self._build_workload_cache()

        if cfg.repos:
            console.rule("[bold]Setting up local environment[/bold]")
            setup_local_env(cfg.repos, env_dir=cfg.local_env_dir)
            console.print()

        self._print_task_table()

        results = []
        for i, cmd in enumerate(cfg.commands):
            task_hash = cfg.task_hash(cmd)
            status = self._process_task(i, cmd, task_hash)
            results.append({"command": cmd.command, "hash": task_hash, "status": status})

        console.print()
        console.rule("[bold green]All tasks processed[/bold green]")
        completed = sum(1 for r in results if r["status"] == "completed")
        console.print(f"  Completed: {completed}/{len(cfg.commands)}")
        console.print()

        if cfg.state_dir:
            self._write_artifact(
                f"run-{cfg.run_hash or 'default'}.json",
                {"run_hash": cfg.run_hash, "tasks": results},
            )

        return results

    # ── Per-task logic ─────────────────────────────────────────────────

    def _process_task(self, index: int, cmd: CommandConfig, task_hash: str) -> str:
        cfg = self.config
        total = len(cfg.commands)

        console.rule(f"Task {index + 1}/{total}")
        console.print(f"  Command: {cmd.command}")
        if cmd.lib:
            console.print(f"  Lib:     {cmd.lib}")
        console.print(f"  Hash:    {task_hash}")

        cached = self._workload_cache.get(task_hash)
        if cached is not None:
            result = self._check_existing_experiment(cached, task_hash)
            if result is not None:
                return result

        if cfg.dry_run:
            console.print("  [dim]Dry run — would execute command[/dim]")
            return "dry_run"

        cwd = str(cfg.repo_dir(cmd.lib)) if cmd.lib else None

        console.print("  [cyan]Running locally...[/cyan]")
        try:
            exp_name, url, exp_id = run_command_and_capture_experiment(
                command=cmd.command,
                cwd=cwd,
            )
        except RuntimeError as e:
            console.print(f"  [red]Error: {e}[/red]")
            return "failed"

        console.print(f"  Experiment: [cyan]{exp_name}[/cyan]")
        console.print(f"  URL: [link={url}]{url}[/link]")

        self._tag_experiment(exp_id, task_hash)

        final = self._wait_for_experiment(exp_id, task_hash)

        if final == "completed":
            console.print("  [green]Task completed successfully.[/green]")
        else:
            console.print(f"  [red]Task ended with status: {final}[/red]")

        return final

    def _check_existing_experiment(
        self, workload: pb2.Workload, task_hash: str
    ) -> Optional[str]:
        """Check a previously-tracked experiment. Returns status to use, or None to re-run."""
        exp_id = workload.experiment.id
        url = self.beaker.workload.url(workload)

        display_status = WORKLOAD_STATUS_DISPLAY.get(workload.status, "unknown")

        if display_status == "completed":
            console.print(f"  [green]Already completed on Beaker — skipping.[/green]")
            console.print(f"  URL: [link={url}]{url}[/link]")
            return "completed"

        try:
            status = get_experiment_status(self.beaker, exp_id)
        except Exception as e:
            console.print(f"  [dim]Could not check previous experiment: {e}[/dim]")
            return None

        if status == "completed":
            console.print(
                "  [green]Previously launched experiment completed — skipping.[/green]"
            )
            console.print(f"  URL: [link={url}]{url}[/link]")
            return "completed"

        if status == "running":
            console.print("  [yellow]Hooking to running experiment...[/yellow]")
            console.print(f"  URL: [link={url}]{url}[/link]")
            final = self._wait_for_experiment(exp_id, task_hash)
            return final

        console.print(f"  [red]Previous run {status} — re-running.[/red]")
        return None

    # ── Display helpers ────────────────────────────────────────────────

    def _print_task_table(self):
        table = Table(title="Tasks", box=None)
        table.add_column("#", style="cyan", width=4)
        table.add_column("Hash", style="yellow", width=14)
        table.add_column("Command", style="white", overflow="fold")
        table.add_column("Status", style="green", width=12)

        for i, cmd in enumerate(self.config.commands):
            task_hash = self.config.task_hash(cmd)
            cached = self._workload_cache.get(task_hash)
            if cached is not None:
                status = WORKLOAD_STATUS_DISPLAY.get(cached.status, "unknown")
            else:
                status = "new"
            display_cmd = cmd.command if len(cmd.command) <= 80 else cmd.command[:77] + "..."
            table.add_row(str(i + 1), task_hash, display_cmd, status)

        console.print(table)
        console.print()

    def _write_artifact(self, filename: str, data: dict):
        if not self.config.state_dir:
            return
        try:
            out_dir = Path(self.config.state_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
            path = out_dir / filename
            with open(path, "w") as f:
                json.dump(data, f, indent=2)
        except OSError as e:
            console.print(f"  [dim]Warning: could not write to state_dir: {e}[/dim]")
