import argparse
import json
from typing import List, Optional, Union

from gantry.api import Recipe

from bipelines.config import BipelineConfig


def launch(
    config: Union[str, BipelineConfig],
    workspace: str,
    budget: str,
    *,
    clusters: Optional[List[str]] = None,
    name: str = "bipelines",
    show_logs: bool = True,
    dry_run: bool = False,
    env: Optional[List[str]] = None,
    secrets: Optional[List[str]] = None,
    extra_args: Optional[List[str]] = None,
) -> Recipe:
    env = env or []
    secrets = secrets or []
    extra_args = extra_args or []

    for ev in env:
        if "=" not in ev:
            raise ValueError(f"Invalid env var format '{ev}', expected KEY=VALUE")
    for sec in secrets:
        if "=" not in sec:
            raise ValueError(f"Invalid secret format '{sec}', expected ENV_VAR=SECRET_NAME")

    if isinstance(config, BipelineConfig):
        task_args = ["bipelines", "--config-json", json.dumps(config.to_dict())] + extra_args
    else:
        task_args = ["bipelines", "--config", config] + extra_args

    recipe = Recipe(
        args=task_args,
        name=name,
        workspace=workspace,
        budget=budget,
        clusters=clusters,
        env_vars=env or None,
        env_secrets=secrets or None,
        yes=True,
    )

    if dry_run:
        recipe.dry_run()
    else:
        recipe.launch(show_logs=show_logs)

    return recipe


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--workspace", type=str, required=True)
    parser.add_argument("--budget", type=str, required=True)
    parser.add_argument("--cluster", type=str, nargs="*")
    parser.add_argument("--name", type=str, default="bipelines")
    parser.add_argument("--show-logs", action="store_true", default=True)
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--env", type=str, nargs="*", default=[], metavar="KEY=VALUE")
    parser.add_argument("--secret", type=str, nargs="*", default=[], metavar="ENV_VAR=SECRET_NAME")

    parser.add_argument(
        "--config", "-c", type=str, required=True,
        help="Path to bipelines YAML config file",
    )

    args, extra = parser.parse_known_args()

    launch(
        config=args.config,
        workspace=args.workspace,
        budget=args.budget,
        clusters=args.cluster,
        name=args.name,
        show_logs=args.show_logs,
        dry_run=args.dry_run,
        env=args.env,
        secrets=args.secret,
        extra_args=extra,
    )


if __name__ == "__main__":
    main()
