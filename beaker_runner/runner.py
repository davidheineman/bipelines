import json
from pathlib import Path

from beaker import Beaker
from rich.console import Console
from rich.table import Table

from beaker_runner.config import RunnerConfig
from beaker_runner.experiment import (
    create_experiment,
    find_matching_experiment,
    get_workload_status,
    list_recent_experiments,
    next_experiment_name,
    wait_for_completion,
)

console = Console()


class Runner:
    def __init__(self, config: RunnerConfig):
        self.config = config
        self.beaker = Beaker.from_env(default_workspace=config.workspace)

    # Test artifact writing (state_dir is for debugging only)
    def _write_test_artifact(self, filename: str, data: dict):
        """Write a JSON artifact to state_dir for testing/debugging."""
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

    # Main loop
    def run(self):
        cfg = self.config

        console.print()
        console.print("[bold]Beaker Runner[/bold]")
        console.print(f"  Workspace:  {cfg.workspace}")
        console.print(f"  Clusters:   {', '.join(cfg.clusters)}")
        console.print(f"  Image:      {cfg.image}")
        console.print(f"  Run hash:   {cfg.run_hash or '(none)'}")
        console.print(f"  Commands:   {len(cfg.commands)}")
        if cfg.state_dir:
            console.print(f"  State dir:  {cfg.state_dir}")
        if cfg.dry_run:
            console.print("  [yellow]DRY RUN — no experiments will be launched[/yellow]")
        console.print()

        experiment_lookup = list_recent_experiments(self.beaker, cfg.workspace, limit=1000)
        console.print()

        self._print_task_table(experiment_lookup)

        results = []
        for i, cmd in enumerate(cfg.commands):
            task_hash = cfg.task_hash(cmd)
            status = self._process_task(i, cmd, task_hash, experiment_lookup)
            results.append({"command": cmd, "hash": task_hash, "status": status})

        console.print()
        console.rule("[bold green]All tasks processed[/bold green]")
        completed = sum(1 for r in results if r["status"] == "completed")
        console.print(f"  Completed: {completed}/{len(cfg.commands)}")
        console.print()

        self._write_test_artifact(
            f"run-{cfg.run_hash or 'default'}.json",
            {"run_hash": cfg.run_hash, "tasks": results},
        )

    # Per-task logic

    def _process_task(
        self, index: int, command: str, task_hash: str, experiment_lookup: dict
    ) -> str:
        cfg = self.config
        total = len(cfg.commands)

        console.rule(f"Task {index + 1}/{total}")
        console.print(f"  Command: {command}")
        console.print(f"  Hash:    {task_hash}")

        # Check Beaker for an existing experiment with this hash
        match_name = find_matching_experiment(
            experiment_lookup, cfg.experiment_prefix, task_hash
        )

        if match_name:
            wl = experiment_lookup[match_name]["workload"]
            status, _ = get_workload_status(self.beaker, wl)

            if status == "completed":
                console.print("  [green]Already completed — skipping.[/green]")
                return "completed"

            if status == "running":
                console.print("  [yellow]Already running — hooking to job...[/yellow]")
                self._print_url(wl)
                final = wait_for_completion(self.beaker, wl)
                self._log_final(final)
                return final

            if status in ("failed", "canceled"):
                console.print(f"  [red]Previous run {status} — will relaunch.[/red]")

        # Launch new experiment
        exp_name = next_experiment_name(experiment_lookup, cfg.experiment_prefix, task_hash)
        script = cfg.setup_script(command)

        if cfg.dry_run:
            console.print(f"  [dim]Dry run — would launch as '{exp_name}'[/dim]")
            console.print(f"  Script preview:\n{script}")
            self._write_test_artifact(f"{exp_name}.sh", {"script": script})
            return "dry_run"

        console.print(f"  Launching [cyan]{exp_name}[/cyan]...")

        workload = create_experiment(
            beaker=self.beaker,
            name=exp_name,
            script=script,
            workspace=cfg.workspace,
            clusters=cfg.clusters,
            budget=cfg.budget,
            image=cfg.image,
            priority=cfg.priority,
            preemptible=cfg.preemptible,
            description=f"{cfg.description} | {task_hash}",
        )

        self._print_url(workload)

        # Add to in-memory lookup so subsequent tasks see it
        experiment_lookup[exp_name] = {"workload": workload, "experiment": None}

        final = wait_for_completion(self.beaker, workload)
        self._log_final(final)
        return final

    # Helpers
    def _print_url(self, workload):
        try:
            url = self.beaker.workload.url(workload)
            console.print(f"  URL: [link={url}]{url}[/link]")
        except Exception:
            pass

    @staticmethod
    def _log_final(status: str):
        if status == "completed":
            console.print("  [green]Task completed successfully.[/green]")
        else:
            console.print(f"  [red]Task ended with status: {status}[/red]")

    def _print_task_table(self, experiment_lookup: dict):
        table = Table(title="Tasks", box=None)
        table.add_column("#", style="cyan", width=4)
        table.add_column("Hash", style="yellow", width=14)
        table.add_column("Command", style="white", overflow="fold")
        table.add_column("Status", style="green", width=12)

        for i, cmd in enumerate(self.config.commands):
            task_hash = self.config.task_hash(cmd)
            match_name = find_matching_experiment(
                experiment_lookup, self.config.experiment_prefix, task_hash
            )

            status = "new"
            if match_name:
                wl = experiment_lookup[match_name]["workload"]
                status, _ = get_workload_status(self.beaker, wl)

            display_cmd = cmd if len(cmd) <= 80 else cmd[:77] + "..."
            table.add_row(str(i + 1), task_hash, display_cmd, status)

        console.print(table)
        console.print()
