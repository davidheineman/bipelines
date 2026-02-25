<img width="150" alt="pipeline" src="https://github.com/user-attachments/assets/e56b5a0e-0c71-4ee9-86c6-767540903664" />

## bipelines

chain together beaker commands with bipelines: pipelines for beaker!

### quick start

```sh
# Install from GitHub
pip install git+https://github.com/davidheineman/bipelines.git

# ... or pull and install
uv sync
```

```sh
bipelines --config example_config.yaml
```

### example usage

```sh
bipelines \
    --command "python launch_eval.py --model A" \
    --command "python launch_eval.py --model B" \
    --run-hash my-eval-v1

# YAML config
bipelines --config config.yaml

# dry run
bipelines --config config.yaml --dry-run

# with repo
bipelines \
    --command "python -m olmo_core.launch --config train.yaml" \
    --repo '{"url":"https://github.com/allenai/OLMo-core","branch":"main","install":"uv pip install -e .[beaker]"}' \
    --run-hash test-v1
```

### gantry usage

```sh
bipelines-launch \
    --workspace ai2/adaptability \
    --budget ai2/oe-base \
    --secret \
        BEAKER_TOKEN=DAVIDH_BEAKER_TOKEN \
        GITHUB_TOKEN=GITHUB_TOKEN \
    -c example_config.yaml
```
