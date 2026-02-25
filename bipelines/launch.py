import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Union

from rich.console import Console

from bipelines.config import BipelineConfig

console = Console()

# Inline script executed inside the clean launch venv.
# Reads Recipe parameters from stdin as JSON, creates and launches the gantry Recipe.
_LAUNCH_SCRIPT = """\
import json, sys
from gantry.api import Recipe

params = json.load(sys.stdin)
recipe = Recipe(
    args=params["args"],
    name=params["name"],
    description=params["description"],
    workspace=params["workspace"],
    budget=params["budget"],
    clusters=params.get("clusters"),
    env_vars=params.get("env_vars"),
    env_secrets=params.get("env_secrets"),
    yes=True,
)
if params.get("dry_run"):
    recipe.dry_run()
else:
    recipe.launch(show_logs=params.get("show_logs", True))
"""


def _get_git_info() -> tuple[str, str]:
    """Return (remote_url, branch) for the current git repo."""
    url = subprocess.check_output(
        ["git", "remote", "get-url", "origin"], text=True
    ).strip()
    branch = (
        subprocess.check_output(
            ["git", "branch", "--show-current"], text=True
        ).strip()
        or "main"
    )
    return url, branch


def _ensure_launch_env(base_dir: str = ".bipelines") -> tuple[Path, str]:
    """Maintain a clean repo clone + venv under base_dir/launch/ for gantry.

    Returns (repo_path, venv_python_path).
    """
    from bipelines.local_env import _find_uv

    launch_root = Path(base_dir).resolve() / "launch"
    repo_path = launch_root / "repo"
    venv_path = launch_root / "venv"
    url, branch = _get_git_info()
    uv = _find_uv()
    venv_python = str(venv_path / "bin" / "python")

    launch_root.mkdir(parents=True, exist_ok=True)

    devnull = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}

    if not repo_path.exists():
        console.print(f"[dim]Cloning {url} (branch {branch}) into launch env...[/dim]")
        subprocess.run(
            ["git", "clone", "--branch", branch, "--depth", "1", url, str(repo_path)],
            check=True, **devnull,
        )
    else:
        subprocess.run(
            ["git", "fetch", "origin", branch, "--depth", "1"],
            cwd=str(repo_path), check=True, **devnull,
        )
        subprocess.run(
            ["git", "checkout", "FETCH_HEAD"],
            cwd=str(repo_path), check=True, **devnull,
        )

    if not venv_path.exists():
        console.print("[dim]Creating launch venv...[/dim]")
        if uv:
            subprocess.run([uv, "venv", str(venv_path)], check=True, **devnull)
        else:
            subprocess.run(
                [sys.executable, "-m", "venv", str(venv_path)], check=True
            )

        console.print("[dim]Installing bipelines into launch venv...[/dim]")
        if uv:
            subprocess.run(
                [uv, "pip", "install", "--python", venv_python, str(repo_path)],
                check=True, **devnull,
            )
        else:
            subprocess.run(
                [venv_python, "-m", "pip", "install", str(repo_path)],
                check=True, **devnull,
            )

    return repo_path, venv_python


def launch(
    config: Union[str, BipelineConfig],
    workspace: str,
    budget: str,
    *,
    clusters: Optional[List[str]] = None,
    name: str = "bipelines",
    description: str = "bipelines parent job",
    show_logs: bool = True,
    dry_run: bool = False,
    env: Optional[List[str]] = None,
    secrets: Optional[List[str]] = None,
    extra_args: Optional[List[str]] = None,
):
    """Launch a bipelines run on Beaker via gantry, from a clean local clone
    so that uncommitted changes in the working tree don't cause gantry warnings.
    """
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

    repo_path, venv_python = _ensure_launch_env()

    params = {
        "args": task_args,
        "name": name,
        "description": description,
        "workspace": workspace,
        "budget": budget,
        "clusters": clusters,
        "env_vars": env or None,
        "env_secrets": secrets or None,
        "show_logs": show_logs,
        "dry_run": dry_run,
    }

    proc = subprocess.run(
        [venv_python, "-c", _LAUNCH_SCRIPT],
        input=json.dumps(params),
        text=True,
        cwd=str(repo_path),
    )

    if proc.returncode != 0:
        raise RuntimeError(f"Launch failed with exit code {proc.returncode}")


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--workspace", type=str, required=True)
    parser.add_argument("--budget", type=str, required=True)
    parser.add_argument("--cluster", type=str, nargs="*")
    parser.add_argument("--name", type=str, default="bipelines")
    parser.add_argument("--description", type=str, default="bipelines parent job")
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
        description=args.description,
        show_logs=args.show_logs,
        dry_run=args.dry_run,
        env=args.env,
        secrets=args.secret,
        extra_args=extra,
    )


if __name__ == "__main__":
    main()
