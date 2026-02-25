import argparse
import json
import sys

from beaker_runner.config import CommandConfig, RepoConfig, RunnerConfig, load_config_from_yaml
from beaker_runner.runner import Runner


def parse_args():
    parser = argparse.ArgumentParser(
        description="Beaker Runner: run commands locally and poll resulting Beaker experiments",
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
    parser.add_argument(
        "--local-env-dir",
        type=str,
        default=".beaker-runner",
        help="Directory for local repo clones and venv (default: .beaker-runner)",
    )

    parser.add_argument(
        "--workspace",
        type=str,
        default=None,
        help="Beaker workspace (e.g. ai2/adaptability) for experiment deduplication",
    )
    parser.add_argument(
        "--run-hash", type=str, default="", help="Unique identifier for this batch of tasks"
    )
    parser.add_argument(
        "--state-dir", type=str, default=None, help="Directory to save run artifacts"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Show what would happen without executing",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    if args.config:
        config = load_config_from_yaml(args.config)
        if args.commands:
            config.commands = [CommandConfig(command=c) for c in args.commands]
        if args.dry_run:
            config.dry_run = True
        if args.state_dir:
            config.state_dir = args.state_dir
        if args.run_hash:
            config.run_hash = args.run_hash
        if args.workspace:
            config.workspace = args.workspace
        if args.local_env_dir != ".beaker-runner":
            config.local_env_dir = args.local_env_dir
    else:
        if not args.commands:
            print("Error: provide at least one --command or use --config", file=sys.stderr)
            sys.exit(1)

        repos = []
        if args.repos:
            for repo_json in args.repos:
                repos.append(RepoConfig(**json.loads(repo_json)))

        config = RunnerConfig(
            commands=[CommandConfig(command=c) for c in args.commands],
            repos=repos,
            workspace=args.workspace,
            run_hash=args.run_hash,
            local_env_dir=args.local_env_dir,
            state_dir=args.state_dir,
            dry_run=args.dry_run,
        )

    runner = Runner(config)
    runner.run()


if __name__ == "__main__":
    main()
