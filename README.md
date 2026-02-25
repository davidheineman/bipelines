chain together beaker commands with beaker runner!

### quick start

```sh
uv sync
```

```sh
brunner --config example_config.yaml
```

### example usage

```sh
brunner \
    --command "python launch_eval.py --model A" \
    --command "python launch_eval.py --model B" \
    --run-hash my-eval-v1

# YAML config
brunner --config config.yaml

# dry run
brunner --config config.yaml --dry-run

# with repo
brunner \
    --command "python -m olmo_core.launch --config train.yaml" \
    --repo '{"url":"https://github.com/allenai/OLMo-core","branch":"main","install":"uv pip install -e .[beaker]"}' \
    --run-hash test-v1
```

### gantry usage

```sh
brunner-launch \
    --config example_config.yaml \
    --workspace ai2/adaptability \
    --budget ai2/oe-base \
    --env BEAKER_TOKEN=DAVID_BEAKER_TOKEN
```
