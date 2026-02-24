import time
from typing import Dict, List, Optional, Tuple

from beaker import (
    Beaker,
    BeakerConstraints,
    BeakerDataMount,
    BeakerDataSource,
    BeakerEnvVar,
    BeakerExperimentSpec,
    BeakerImageSource,
    BeakerJobPriority,
    BeakerResultSpec,
    BeakerTaskContext,
    BeakerTaskResources,
    BeakerTaskSpec,
    BeakerWorkloadStatus,
    BeakerWorkloadType,
)
from cuvette.constants.secrets import USER_ENV_SECRETS, USER_FILE_SECRETS
from rich.console import Console

console = Console()

WEKA_MOUNTS = [
    BeakerDataMount(mount_path="/oe-eval-default", source=BeakerDataSource(weka="oe-eval-default")),
    BeakerDataMount(
        mount_path="/oe-training-default", source=BeakerDataSource(weka="oe-training-default")
    ),
    BeakerDataMount(
        mount_path="/oe-adapt-default", source=BeakerDataSource(weka="oe-adapt-default")
    ),
]

PRIORITY_MAP = {
    "urgent": BeakerJobPriority.urgent,
    "high": BeakerJobPriority.high,
    "normal": BeakerJobPriority.normal,
    "low": BeakerJobPriority.low,
}


def _build_datasets() -> List[BeakerDataMount]:
    datasets = list(WEKA_MOUNTS)

    dst_seen: set[str] = set()
    for secret in USER_FILE_SECRETS:
        mount_path = f"/root/{secret['path']}"
        if mount_path in dst_seen:
            continue
        dst_seen.add(mount_path)
        datasets.append(
            BeakerDataMount(mount_path=mount_path, source=BeakerDataSource(secret=secret["name"]))
        )

    return datasets


def _build_env_vars() -> List[BeakerEnvVar]:
    return [BeakerEnvVar(name=s["env"], secret=s["name"]) for s in USER_ENV_SECRETS]


def create_experiment(
    beaker: Beaker,
    name: str,
    script: str,
    workspace: str,
    clusters: List[str],
    budget: str,
    image: str,
    priority: str = "normal",
    preemptible: bool = False,
    description: str = "",
):
    """Create a CPU experiment on Beaker (0 GPUs, weka mounts, user secrets)."""
    task = BeakerTaskSpec(
        name=name,
        image=BeakerImageSource(beaker=image),
        command=["bash", "-c", script],
        host_networking=True,
        result=BeakerResultSpec(path="/output"),
        datasets=_build_datasets(),
        env_vars=_build_env_vars(),
        constraints=BeakerConstraints(cluster=clusters),
        context=BeakerTaskContext(
            priority=PRIORITY_MAP[priority],
            preemptible=preemptible,
        ),
        resources=BeakerTaskResources(gpu_count=0),
    )

    spec = BeakerExperimentSpec(
        tasks=[task],
        description=description,
        budget=budget,
    )

    workload = beaker.experiment.create(spec=spec, name=name, workspace=workspace)
    return workload


def list_recent_experiments(
    beaker: Beaker,
    workspace: str,
    limit: int = 1000,
) -> Dict[str, dict]:
    """
    Pull recent experiments from the workspace and return a name -> info lookup.
    Used for dedup: if an experiment with matching name exists, we can skip or hook to it.
    """
    console.print(f"  Pulling last {limit} experiments from [cyan]{workspace}[/cyan]...")

    workspace_obj = beaker.workspace.get(workspace)
    user = beaker.user.get(beaker.user_name)

    workloads = list(
        beaker.workload.list(
            workspace=workspace_obj,
            author=user,
            workload_type=BeakerWorkloadType.experiment,
            limit=limit,
        )
    )

    lookup: Dict[str, dict] = {}
    for workload in workloads:
        if not beaker.workload.is_experiment(workload):
            continue
        exp = workload.experiment
        name = getattr(exp, "name", None)
        if name:
            lookup[name] = {"workload": workload, "experiment": exp}

    console.print(f"  Found [green]{len(lookup)}[/green] named experiments")
    return lookup


def get_workload_status(beaker: Beaker, workload) -> Tuple[str, Optional[object]]:
    """Return (status_string, job_or_None) for a workload."""
    job = beaker.workload.get_latest_job(workload)

    if job is None:
        return "pending", None

    status = job.status.status

    STATUS_MAP = {
        BeakerWorkloadStatus.running: "running",
        BeakerWorkloadStatus.succeeded: "completed",
        BeakerWorkloadStatus.failed: "failed",
        BeakerWorkloadStatus.canceled: "canceled",
    }

    return STATUS_MAP.get(status, "unknown"), job


def wait_for_completion(
    beaker: Beaker,
    workload,
    poll_interval: float = 15.0,
) -> str:
    """Poll a workload until it reaches a terminal state. Returns final status string."""
    last_status = None

    while True:
        status, _ = get_workload_status(beaker, workload)

        if status != last_status:
            console.print(f"  Status: [yellow]{status}[/yellow]")
            last_status = status

        if status in ("completed", "failed", "canceled"):
            return status

        time.sleep(poll_interval)


def find_matching_experiment(
    experiment_lookup: Dict[str, dict],
    experiment_prefix: str,
    task_hash: str,
) -> Optional[str]:
    """
    Find the best matching experiment name for a given task hash.
    Checks base name and retry suffixes (e.g. beaker-runner-{hash}, beaker-runner-{hash}-1, ...).
    Returns the name, or None if not found.
    """
    base_name = f"{experiment_prefix}-{task_hash}"

    latest_match = base_name if base_name in experiment_lookup else None

    n = 1
    while True:
        retry_name = f"{base_name}-{n}"
        if retry_name in experiment_lookup:
            latest_match = retry_name
            n += 1
        else:
            break

    return latest_match


def next_experiment_name(
    experiment_lookup: Dict[str, dict],
    experiment_prefix: str,
    task_hash: str,
) -> str:
    """Generate the next available experiment name (handles retries after failures)."""
    base_name = f"{experiment_prefix}-{task_hash}"
    if base_name not in experiment_lookup:
        return base_name

    n = 1
    while f"{base_name}-{n}" in experiment_lookup:
        n += 1
    return f"{base_name}-{n}"
