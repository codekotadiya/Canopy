# Contributing to Canopy

Thanks for your interest in contributing to Canopy. This guide covers everything you need to get started.

## Development Setup

```bash
# Clone the repo
git clone https://github.com/YOUR_ORG/canopy.git
cd canopy

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# Install in editable mode with dev dependencies
pip install -e ".[dev]"

# Verify installation
canopy --help
pytest
```

## Running Tests

```bash
# All tests
pytest

# With coverage
pytest --cov=canopy

# Specific test file
pytest tests/test_csv_connector.py -v
```

Tests use SQLite as a stand-in for PostgreSQL (via SQLAlchemy's dialect abstraction) and mock the LLM provider, so you don't need a running database or Ollama to run the test suite.

## Code Style

We use [Ruff](https://docs.astral.sh/ruff/) for linting and formatting:

```bash
ruff check .
ruff format .
```

Line length limit is 100 characters. Target Python version is 3.11+.

## How to Contribute

### Reporting Bugs

Open an issue with:
- What you expected to happen
- What actually happened
- Steps to reproduce
- Your environment (OS, Python version, Ollama model if relevant)

### Suggesting Features

Open an issue describing the use case. We're especially interested in:
- New source connectors (JSON, XML, SQL databases, APIs)
- New target loaders (MySQL, SQLite, Snowflake, BigQuery)
- New LLM providers (OpenAI, Anthropic, local models)
- Improvements to the agentic review loop

### Submitting a Pull Request

1. Fork the repo and create a branch from `main`
2. Make your changes
3. Add or update tests for your changes
4. Run `pytest` and `ruff check .` to make sure everything passes
5. Open a PR with a clear description of what you changed and why

## Adding a Source Connector

Source connectors read data from external systems. To add one:

1. Create `canopy/core/ingestion/your_connector.py`
2. Implement the `BaseConnector` abstract class:

```python
from canopy.core.ingestion.base import BaseConnector

class YourConnector(BaseConnector):
    def __init__(self, config: SourceConfig) -> None:
        # Initialize from config
        ...

    def read_sample(self, n: int = 50) -> list[dict[str, str]]:
        # Return up to n rows as list of dicts (all values as strings)
        ...

    def read_all(self, chunk_size: int = 1000) -> Iterator[list[dict[str, str]]]:
        # Yield chunks of rows. Must stream — never load full dataset into memory.
        ...

    def get_raw_columns(self) -> list[str]:
        # Return column names from the source
        ...
```

3. Register it in `canopy/core/context/factories.py`:

```python
def create_connector(config: PipelineConfig) -> BaseConnector:
    source_type = config.source.type.lower()
    if source_type == "your_type":
        from canopy.core.ingestion.your_connector import YourConnector
        return YourConnector(config.source)
    ...
```

4. Add tests in `tests/test_your_connector.py`
5. Update the `SourceConfig` model in `canopy/models/config.py` if your connector needs new config fields

**Reference implementation:** `canopy/core/ingestion/csv_connector.py`

## Adding an LLM Provider

LLM providers handle communication with inference backends. To add one:

1. Create `canopy/llm/your_provider.py`
2. Implement the `BaseLLMProvider` abstract class:

```python
from canopy.llm.base import BaseLLMProvider

class YourProvider(BaseLLMProvider):
    def __init__(self, config: LLMConfig) -> None:
        ...

    def complete(self, prompt: str, system: str | None = None) -> str:
        # Send prompt to the LLM, return the text response
        ...

    def is_cloud(self) -> bool:
        # Return True if data leaves the local machine
        # This triggers a privacy warning for users
        ...
```

3. Register it in `canopy/core/context/factories.py`
4. Add tests with mocked HTTP calls (see `tests/test_ollama_provider.py` for the pattern using `respx`)

**Reference implementation:** `canopy/llm/ollama.py`

## Adding a Target Loader

Target loaders write normalized data to databases. To add one:

1. Create `canopy/core/loader/your_loader.py`
2. Implement the `BaseLoader` abstract class:

```python
from canopy.core.loader.base import BaseLoader

class YourLoader(BaseLoader):
    def __init__(self, connection_string: str) -> None:
        ...

    def get_target_schema(self, table_name: str) -> TargetSchema | None:
        # Reflect the existing table schema. Return None if table doesn't exist.
        ...

    def ensure_table(self, schema: TargetSchema) -> None:
        # Create the table from schema if it doesn't exist. Idempotent.
        ...

    def load_batch(self, table_name: str, rows: list[dict[str, Any]]) -> int:
        # Bulk insert rows. Return count of rows inserted.
        ...

    def finalize(self) -> LoadSummary:
        # Commit, close connections, return summary.
        ...
```

3. Register it in `canopy/core/context/factories.py`
4. Add tests (using SQLite as a stand-in if your loader uses SQLAlchemy)

**Reference implementation:** `canopy/core/loader/postgres.py`

## Project Structure

```
canopy/
├── core/
│   ├── ingestion/      # Source connectors (BaseConnector implementations)
│   ├── context/        # Agentic engine, LLM prompts, response parsers, factories
│   ├── script_gen/     # Script template, generator, and runner
│   └── loader/         # Target DB loaders (BaseLoader implementations)
├── llm/                # LLM providers (BaseLLMProvider implementations)
├── models/             # Pydantic models (config, schema, analysis, execution)
├── triggers/           # CLI entry point
└── config/             # Sample pipeline configs
```

## Design Principles

- **LLM generates code, not data.** The LLM writes a conversion script once; all row processing is deterministic Python. This keeps things token-efficient and reproducible.
- **Plugin interfaces over configuration.** New capabilities are added by implementing an ABC, not by adding flags to existing code.
- **Zero data persistence by default.** Canopy never stores customer data outside the configured target database.
- **Tests don't need infrastructure.** Unit and integration tests run with SQLite and mocked LLM responses.

## Good First Issues

Look for issues labeled `good first issue` — these are scoped and documented for new contributors. Typical examples:

- Add a new source connector (e.g., JSON, TSV)
- Add a new LLM provider (e.g., OpenAI)
- Improve type coercion in generated scripts
- Add a CLI flag or config option
