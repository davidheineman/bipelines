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


def create_venv(env_dir: str = ".bipelines") -> Path:
    """Create a virtual environment under env_dir/venv. Returns the venv path."""
    env_path = Path(env_dir).resolve()
    venv_path = env_path / "venv"

    if venv_path.exists():
        console.print(f"  Using existing venv at [cyan]{venv_path}[/cyan]")
        return venv_path

    console.print(f"  Creating venv at [cyan]{venv_path}[/cyan]...")
    env_path.mkdir(parents=True, exist_ok=True)

    uv = _find_uv()
    if uv:
        subprocess.run([uv, "venv", str(venv_path)], check=True)
    else:
        subprocess.run([sys.executable, "-m", "venv", str(venv_path)], check=True)

    return venv_path


def get_venv_env(venv_path: Path) -> dict:
    """Return an env dict that activates the given venv (sets PATH, VIRTUAL_ENV)."""
    env = _env_with_uv()
    venv_bin = str(venv_path / "bin")
    env["VIRTUAL_ENV"] = str(venv_path)
    env["PATH"] = f"{venv_bin}:{env.get('PATH', '')}"
    env.pop("PYTHONHOME", None)
    return env


def setup_local_env(
    repos: List[RepoConfig],
    env_dir: str = ".bipelines",
) -> Path:
    """Create a venv, clone repos, and install them into the venv. Returns the venv path."""
    env_path = Path(env_dir).resolve()
    repos_path = env_path / "repos"
    repos_path.mkdir(parents=True, exist_ok=True)

    venv_path = create_venv(env_dir)
    install_env = get_venv_env(venv_path)

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

    return venv_path
