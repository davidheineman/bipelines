### quick start

```sh
uv sync
```

```sh
beaker-runner --config example_config.yaml
```

### example usage

```sh
# Run commands directly
beaker-runner \
    --command "python eval.py --model A" \
    --command "python eval.py --model B" \
    --run-hash my-eval-v1

# Run from YAML config
beaker-runner --config config.yaml

# Dry run (no experiments launched)
beaker-runner --config config.yaml --dry-run

# With repo setup
beaker-runner \
    --command "cd ~/repos/myrepo && python eval.py" \
    --repo '{"url":"https://github.com/user/myrepo","branch":"main","install":"uv pip install -e ."}' \
    --run-hash test-v1

# Save/load state for testing
beaker-runner --config config.yaml \
    --state-dir /oe-eval-default/davidh/tmp/beaker-runner-test
```