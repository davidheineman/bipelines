import hashlib
from dataclasses import dataclass, field
from typing import List, Optional

import yaml


@dataclass
class RepoConfig:
    """A git repository to clone and install inside the Beaker job."""

    url: str
    branch: str = "main"
    commit: Optional[str] = None
    install: Optional[str] = None
    path: Optional[str] = None

    @property
    def clone_path(self) -> str:
        if self.path:
            return self.path
        name = self.url.rstrip("/").split("/")[-1].removesuffix(".git")
        return f"~/repos/{name}"


@dataclass
class RunnerConfig:
    """Main configuration for the beaker-runner orchestrator."""

    commands: List[str]
    repos: List[RepoConfig] = field(default_factory=list)

    workspace: str = "ai2/adaptability"
    clusters: List[str] = field(default_factory=lambda: ["ai2/saturn"])
    budget: str = "ai2/oe-base"
    image: str = "ai2/cuda12.8-dev-ubuntu22.04-torch2.7.1"
    priority: str = "normal"
    preemptible: bool = False

    run_hash: str = ""
    experiment_prefix: str = "beaker-runner"
    description: str = "beaker-runner sequential task"

    state_dir: Optional[str] = None
    dry_run: bool = False

    def task_hash(self, command: str) -> str:
        """Deterministic hash for deduplication: command + run_hash."""
        content = f"{command}|{self.run_hash}"
        return hashlib.sha256(content.encode()).hexdigest()[:12]

    def experiment_name(self, command: str) -> str:
        return f"{self.experiment_prefix}-{self.task_hash(command)}"

    def setup_script(self, command: str) -> str:
        """Build the bash script that clones repos, installs, then runs the command."""
        lines = ["#!/bin/bash", "set -eo pipefail", ""]

        if self.repos:
            lines.append("mkdir -p ~/repos")
            lines.append("")

            for repo in self.repos:
                clone_path = repo.clone_path
                lines.append(f"echo '--- cloning {repo.url} ---'")
                lines.append(f"git clone {repo.url} {clone_path}")
                lines.append(f"cd {clone_path}")

                if repo.commit:
                    lines.append(f"git checkout {repo.commit}")
                elif repo.branch and repo.branch != "main":
                    lines.append(f"git checkout {repo.branch}")

                if repo.install:
                    lines.append(f"echo '--- installing {clone_path} ---'")
                    lines.append(repo.install)

                lines.append("cd /")
                lines.append("")

        lines.append(f"echo '--- running command ---'")
        lines.append(command)
        return "\n".join(lines)


def load_config_from_yaml(path: str) -> RunnerConfig:
    with open(path) as f:
        data = yaml.safe_load(f)

    kwargs = {k: v for k, v in data.items() if k != "repos"}
    kwargs["repos"] = [RepoConfig(**r) for r in data.get("repos", [])]

    return RunnerConfig(**kwargs)
