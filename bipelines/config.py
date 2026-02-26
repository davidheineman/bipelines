import hashlib
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pathlib import Path

import yaml


@dataclass
class RepoConfig:
    """A git repository to clone and install into the local environment."""

    url: str
    branch: str = "main"
    commit: Optional[str] = None
    install: Optional[str] = None
    path: Optional[str] = None

    @property
    def name(self) -> str:
        return self.url.rstrip("/").split("/")[-1].removesuffix(".git")


@dataclass
class CommandConfig:
    """A command to run locally that launches a Beaker experiment."""

    command: str
    lib: Optional[str] = None
    raw: bool = False


@dataclass
class BipelineConfig:
    """Main configuration for the bipelines orchestrator."""

    commands: List[CommandConfig]
    repos: List[RepoConfig] = field(default_factory=list)

    workspace: Optional[str] = None
    run_hash: str = ""

    local_env_dir: str = ".bipelines"
    state_dir: Optional[str] = None
    dry_run: bool = False

    @property
    def repo_lookup(self) -> Dict[str, RepoConfig]:
        return {r.name: r for r in self.repos}

    def validate(self):
        """Check that all lib references in commands point to known repos."""
        repo_names = {r.name for r in self.repos}
        for cmd in self.commands:
            if cmd.lib and cmd.lib not in repo_names:
                raise ValueError(
                    f"Command references unknown lib '{cmd.lib}'. "
                    f"Available repos: {', '.join(sorted(repo_names))}"
                )

    def repo_dir(self, repo_name: str) -> Path:
        """Resolve the on-disk path for a cloned repo."""
        return Path(self.local_env_dir).resolve() / "repos" / repo_name

    def task_hash(self, cmd: CommandConfig) -> str:
        """Deterministic hash for deduplication: command + run_hash."""
        content = f"{cmd.command}|{self.run_hash}"
        return hashlib.sha256(content.encode()).hexdigest()[:12]

    def to_dict(self) -> dict:
        """Serialize to a plain dict suitable for YAML output."""
        d: dict = {}
        if self.run_hash:
            d["run_hash"] = self.run_hash
        if self.workspace:
            d["workspace"] = self.workspace
        if self.local_env_dir != ".bipelines":
            d["local_env_dir"] = self.local_env_dir
        if self.state_dir:
            d["state_dir"] = self.state_dir
        if self.dry_run:
            d["dry_run"] = self.dry_run
        if self.repos:
            d["repos"] = [
                {k: v for k, v in r.__dict__.items() if v is not None and k != "name"}
                for r in self.repos
            ]
        d["commands"] = [
            {k: v for k, v in c.__dict__.items() if v is not None}
            for c in self.commands
        ]
        return d

    def to_yaml(self, path: str) -> str:
        """Write this config to a YAML file and return the path."""
        with open(path, "w") as f:
            yaml.dump(self.to_dict(), f, default_flow_style=False, sort_keys=False)
        return path


def load_config_from_dict(data: dict) -> BipelineConfig:
    kwargs = {k: v for k, v in data.items() if k not in ("repos", "commands")}
    kwargs["repos"] = [RepoConfig(**r) for r in data.get("repos", [])]

    commands = []
    for c in data.get("commands", []):
        if isinstance(c, str):
            commands.append(CommandConfig(command=c))
        elif isinstance(c, dict):
            commands.append(CommandConfig(**c))
        else:
            raise ValueError(f"Invalid command entry: {c!r}")
    kwargs["commands"] = commands

    config = BipelineConfig(**kwargs)
    config.validate()
    return config


def load_config_from_yaml(path: str) -> BipelineConfig:
    with open(path) as f:
        data = yaml.safe_load(f)
    return load_config_from_dict(data)
