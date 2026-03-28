# Canopy

*Understands your data. Not just moves it.*

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)

![Canopy overview](assets/overview.svg)

Canopy is an open-source, AI-powered data normalization pipeline. It reads messy data from any source, uses an LLM to understand the schema and generate a deterministic Python conversion script, then runs that script across your full dataset and loads clean data into your database.

The LLM never touches your production data directly. It analyzes a sample, writes code, reviews its own output, and hands off a pure Python script that runs without any further LLM calls.

---

## Why Canopy?

Data engineers spend too much time writing one-off normalization scripts for every new data source. Canopy automates this:

- **Understands context** — The LLM reads your messy CSV, figures out that `$75,000` is a salary, `01/15/1990` is a date, and `Yes`/`true`/`1` all mean the same boolean
- **Generates real code** — Instead of black-box transformations, you get a readable `.py` file you can inspect, edit, and commit
- **Stays deterministic** — The generated script produces identical output every time, with zero LLM calls at runtime
- **Respects your data** — Zero persistence by default. Only the configured sample (default 50 rows) is ever sent to the LLM. Everything else stays local.

## How It Works

```
                         Agentic Loop (LLM)
                    ┌───────────────────────────┐
  Source            │  1. Understand source data │
  (messy CSV) ────► │  2. Inspect target schema  │
                    │  3. Generate script         │
                    │  4. Run on sample           │
                    │  5. Review & self-correct   │
                    └─────────────┬─────────────┘
                                  │ approved script
                                  ▼
                    ┌───────────────────────────┐
  Full dataset ───► │  Execute deterministically │ ───► Target DB
                    │  (no LLM, streaming)       │      (clean)
                    └───────────────────────────┘
```

**Total LLM calls per pipeline run: 4-8.** Not thousands. Not per-row. Just a handful during the planning phase.

## Quickstart

### Prerequisites

- Python 3.11+
- [Ollama](https://ollama.com) running locally (or any supported LLM provider)
- A target database (PostgreSQL, or SQLite for testing)

### Install

```bash
git clone https://github.com/YOUR_ORG/canopy.git
cd canopy
pip install -e ".[dev]"
```

### Pull an Ollama model

```bash
ollama pull llama3
```

### Configure a pipeline

```bash
cp examples/employees_to_postgres.yaml my_pipeline.yaml
```

Edit `my_pipeline.yaml` — set your database connection string and source file path. Connection strings support environment variables via `${VAR_NAME}` syntax.

### Run

```bash
# Validate your config first
canopy validate my_pipeline.yaml

# Run the full pipeline
canopy run my_pipeline.yaml
```

Canopy will:
1. Read a sample of your data
2. Ask the LLM to analyze it and generate a conversion script
3. Test the script on the sample and let the LLM review the output
4. Run the approved script on your full dataset
5. Load clean rows into your database

The generated script is saved to `./scripts/<job_id>_convert.py`. You can inspect, edit, and re-run it:

```bash
# Re-run without any LLM involvement
canopy rerun scripts/<job_id>_convert.py my_pipeline.yaml
```

## Pipeline Configuration

```yaml
name: my_pipeline

source:
  type: csv                    # Source connector type
  path: ./data/input.csv       # Path to source file
  delimiter: ","               # CSV delimiter (default: ,)
  sample_size: 50              # Rows sent to LLM for analysis (default: 50)

target:
  type: postgres               # Target loader type
  connection_string: postgresql://${DB_USER}:${DB_PASSWORD}@localhost/mydb
  table_name: output_table     # Target table name
  create_if_missing: true      # Auto-create table from LLM-proposed schema

llm:
  provider: ollama             # LLM provider (ollama)
  model: llama3                # Model name
  base_url: http://localhost:11434
  temperature: 0.1             # Low temp for deterministic output

script:
  output_dir: ./scripts        # Where generated scripts are saved
  max_review_iterations: 3     # Max LLM review cycles

chunk_size: 1000               # Rows per batch during full execution
```

## CLI Reference

| Command | Description |
|---------|-------------|
| `canopy run <config.yaml>` | Run the full agentic pipeline |
| `canopy validate <config.yaml>` | Validate config without running |
| `canopy rerun <script.py> <config.yaml>` | Re-execute a saved script (no LLM) |

## Architecture

```
canopy/
├── core/
│   ├── ingestion/      # Source connectors (BaseConnector ABC)
│   ├── context/        # Agentic engine, LLM prompts, response parsers
│   ├── script_gen/     # Script template, generator, runner
│   └── loader/         # Target loaders (BaseLoader ABC)
├── llm/                # LLM providers (BaseLLMProvider ABC)
├── models/             # Pydantic data models
├── triggers/           # CLI
└── config/             # Sample pipeline configs
```

### Design Principles

1. **LLM generates code, not data.** The LLM writes a conversion script once. All row processing is pure deterministic Python.
2. **Plugin interfaces over configuration.** New connectors, providers, and loaders are added by implementing an abstract base class — no core modifications needed.
3. **Zero data persistence by default.** Only the configured sample is sent to the LLM. Nothing is stored outside the target database.
4. **Tests don't need infrastructure.** The full test suite runs with SQLite and mocked LLM responses.

## Extending Canopy

Canopy is built around three plugin interfaces. Each is an abstract base class you implement to add support for a new source, LLM provider, or target database.

| Interface | File | Purpose |
|-----------|------|---------|
| `BaseConnector` | `canopy/core/ingestion/base.py` | Read data from a source |
| `BaseLLMProvider` | `canopy/llm/base.py` | Communicate with an LLM |
| `BaseLoader` | `canopy/core/loader/base.py` | Write data to a target |

See [CONTRIBUTING.md](CONTRIBUTING.md) for step-by-step instructions and reference implementations.

### Currently Supported

| Layer | Supported | Planned |
|-------|-----------|---------|
| **Sources** | CSV | JSON, XML, SQL databases, PDF, DOCX |
| **LLM Providers** | Ollama (local) | OpenAI, Anthropic |
| **Targets** | PostgreSQL, SQLite | MySQL, Snowflake, BigQuery |

## Contributing

We welcome contributions! Whether it's a bug report, a new connector, or an improvement to the agentic loop — we'd love your help.

- Read the [Contributing Guide](CONTRIBUTING.md)
- Look for [`good first issue`](../../labels/good%20first%20issue) labels
- Join the discussion in [Issues](../../issues)

## Security

Canopy generates and executes Python code produced by an LLM. The following safeguards are in place:

- **AST validation** — Generated scripts are parsed and checked for dangerous imports, blocked builtins (`eval`, `exec`, `open`, …), and suspicious attribute access *before* any execution.
- **Subprocess isolation** — Scripts run in a separate subprocess with a minimal environment (no inherited secrets), a timeout, and restricted `PATH`.
- **Strict review gate** — The LLM review step is fail-closed: if the review response cannot be parsed, the script is **rejected** (not approved). Unapproved scripts are never executed on the full dataset.
- **Row-level fallback** — If a batch insert fails, the loader retries row-by-row and quarantines failing rows instead of aborting the entire job.

**Current limitations:**
- Scripts are not yet executed in a container or full OS-level sandbox. The subprocess shares the host filesystem.
- The import allow-list covers common data-processing modules. If your transforms need additional libraries, extend `ALLOWED_MODULES` in `canopy/core/script_gen/validator.py`.
- Do not use Canopy with untrusted prompt sources in an unsupervised environment.

## Privacy

Canopy is designed with data sovereignty in mind:

- **Only sample rows** (default 50) are sent to the LLM — never the full dataset
- **Cloud LLM warning** — If you configure a cloud provider, Canopy prints a clear warning before sending any data
- **Ollama by default** — The default provider runs locally. Your data never leaves your machine.
- **No telemetry, no logging, no phone-home** — Canopy does not collect any usage data

## License

[MIT](LICENSE)
