import argparse
import json
import sys

from beaker_runner.config import RunnerConfig, RepoConfig, load_config_from_yaml
from beaker_runner.runner import Runner


def parse_args():
    parser = argparse.ArgumentParser(
        description="Beaker Runner: sequential CPU job orchestrator with deduplication",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--config", "-c", type=str, help="Path to YAML config file")
    parser.add_argument(
        "--command",
        action="append",
        dest="commands",
        help="Command to run (repeatable, executed sequentially)",
    )
    parser.add_argument(
        "--repo",
        action="append",
        dest="repos",
        help='Repo config as JSON: \'{"url":"...","branch":"main","install":"..."}\'',
    )

    parser.add_argument("--workspace", "-w", type=str, default="ai2/adaptability")
    parser.add_argument("--cluster", action="append", dest="clusters")
    parser.add_argument("--budget", type=str, default="ai2/oe-base")
    parser.add_argument("--image", type=str, default="ai2/cuda12.8-dev-ubuntu22.04-torch2.7.1")
    parser.add_argument(
        "--priority", type=str, default="normal", choices=["urgent", "high", "normal", "low"]
    )
    parser.add_argument("--preemptible", action="store_true", default=False)

    parser.add_argument(
        "--run-hash", type=str, default="", help="Unique identifier for this batch of tasks"
    )
    parser.add_argument("--experiment-prefix", type=str, default="beaker-runner")
    parser.add_argument("--description", type=str, default="beaker-runner sequential task")

    parser.add_argument(
        "--state-dir", type=str, default=None, help="Directory to save/load state for resume"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Show what would happen without launching",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    if args.config:
        config = load_config_from_yaml(args.config)
        if args.commands:
            config.commands = args.commands
        if args.dry_run:
            config.dry_run = True
        if args.state_dir:
            config.state_dir = args.state_dir
        if args.run_hash:
            config.run_hash = args.run_hash
    else:
        if not args.commands:
            print("Error: provide at least one --command or use --config", file=sys.stderr)
            sys.exit(1)

        repos = []
        if args.repos:
            for repo_json in args.repos:
                repos.append(RepoConfig(**json.loads(repo_json)))

        config = RunnerConfig(
            commands=args.commands,
            repos=repos,
            workspace=args.workspace,
            clusters=args.clusters or ["ai2/saturn"],
            budget=args.budget,
            image=args.image,
            priority=args.priority,
            preemptible=args.preemptible,
            run_hash=args.run_hash,
            experiment_prefix=args.experiment_prefix,
            description=args.description,
            state_dir=args.state_dir,
            dry_run=args.dry_run,
        )

    runner = Runner(config)
    runner.run()


if __name__ == "__main__":
    main()
