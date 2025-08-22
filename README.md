# Codebase Time Machine

Navigate a codebase through time: index full git history, analyze ownership/churn/complexity, answer evolution/why questions, and generate visualizations.

## Quickstart

```bash
python -m codebase_time_machine.cli --help
```

Initialize and fully index a repository:

```bash
python -m codebase_time_machine.cli init \
  --repo-url https://github.com/owner/repo.git \
  --workdir /workspace/repos/repo \
  --db /workspace/data/ctm.sqlite
```

Ask questions:

```bash
python -m codebase_time_machine.cli query \
  --db /workspace/data/ctm.sqlite \
  "show me how auth evolved"
```

Generate visualizations:

```bash
python -m codebase_time_machine.cli visualize \
  --db /workspace/data/ctm.sqlite \
  --outdir /workspace/output
```
