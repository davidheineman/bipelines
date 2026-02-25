import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List

from rich.console import Console

from bipelines.config import RepoConfig

console = Console()


_UV_SEARCH_DIRS = [
    Path(sys.prefix) / "bin",
    Path.home() / ".local" / "bin",
    Path("/usr/local/bin"),
    Path.home() / ".cargo" / "bin",
]


def _find_uv() -> str | None:
    """Return the absolute path to uv, searching PATH then common install locations."""
    found = shutil.which("uv")
    if found:
        return found
    for d in _UV_SEARCH_DIRS:
        candidate = d / "uv"
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    return None


def _env_with_uv() -> dict:
    """Return a copy of os.environ with uv's directory on PATH (if found)."""
    env = {**os.environ}
    uv = _find_uv()
    if uv:
        uv_dir = str(Path(uv).parent)
        env["PATH"] = f"{uv_dir}:{env.get('PATH', '')}"
    return env


def setup_local_env(
    repos: List[RepoConfig],
    env_dir: str = ".bipelines",
) -> None:
    """Clone repos and install them into the current environment."""
    env_path = Path(env_dir).resolve()
    repos_path = env_path / "repos"
    repos_path.mkdir(parents=True, exist_ok=True)

    install_env = _env_with_uv()

    for repo in repos:
        repo_path = repos_path / repo.name

        if not repo_path.exists():
            console.print(f"  Cloning [cyan]{repo.url}[/cyan]...")
            subprocess.run(
                ["git", "clone", repo.url, str(repo_path)],
                check=True,
            )

        if repo.commit:
            console.print(f"  Checking out commit [yellow]{repo.commit[:12]}[/yellow]...")
            subprocess.run(
                ["git", "checkout", repo.commit],
                cwd=str(repo_path),
                check=True,
            )
        elif repo.branch:
            console.print(f"  Checking out branch [yellow]{repo.branch}[/yellow]...")
            subprocess.run(
                ["git", "fetch", "origin", repo.branch],
                cwd=str(repo_path),
                check=True,
            )
            subprocess.run(
                ["git", "checkout", repo.branch],
                cwd=str(repo_path),
                check=True,
            )
            subprocess.run(
                ["git", "pull", "--ff-only"],
                cwd=str(repo_path),
                check=True,
            )

        if repo.install:
            console.print(f"  Installing [cyan]{repo.name}[/cyan]: {repo.install}")
            subprocess.run(
                repo.install,
                shell=True,
                cwd=str(repo_path),
                env=install_env,
                check=True,
            )
