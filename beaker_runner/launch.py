import argparse

from gantry.api import Recipe


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--workspace", type=str, required=True)
    parser.add_argument("--budget", type=str, required=True)
    parser.add_argument("--cluster", type=str, nargs="*")
    parser.add_argument("--name", type=str, default="beaker-runner")
    parser.add_argument("--show-logs", action="store_true", default=True)
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--env", type=str, nargs="*", default=[], metavar="KEY=VALUE")
    parser.add_argument("--secret", type=str, nargs="*", default=[], metavar="ENV_VAR=SECRET_NAME")

    parser.add_argument(
        "--config", "-c", type=str, required=True,
        help="Path to brunner YAML config file",
    )

    args, extra = parser.parse_known_args()

    for ev in args.env:
        if "=" not in ev:
            parser.error(f"Invalid env var format '{ev}', expected KEY=VALUE")

    for sec in args.secret:
        if "=" not in sec:
            parser.error(f"Invalid secret format '{sec}', expected ENV_VAR=SECRET_NAME")

    task_args = ["brunner", "--config", args.config] + extra

    recipe = Recipe(
        args=task_args,
        name=args.name,
        workspace=args.workspace,
        budget=args.budget,
        clusters=args.cluster,
        env_vars=args.env or None,
        env_secrets=args.secret or None,
        yes=True,
    )

    if args.dry_run:
        recipe.dry_run()
    else:
        recipe.launch(show_logs=args.show_logs)


if __name__ == "__main__":
    main()
