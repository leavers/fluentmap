# Fluentmap - AI Agent Guide

## Project Overview

Fluentmap is a Python library that provides a drop-in replacement for the builtin
`map` with optional executor-based parallel processing and batch/chunk handling.
It works with any object exposing `submit(fn, *args, **kwargs) -> Future`, including
`concurrent.futures` executors.

**Key Design Philosophy**: Single-file design (`fluentmap.py`) with zero runtime
dependencies - keep the code self-contained and easy for users to vendor.

### Main Components

1. **`map()`** - Public generator with the same call signature as builtin `map`,
   plus `executor`, `batch_size`, `chunk_size`, `num_prepare`,
   `sort_by_completion` and `on_return`.

2. **`Arguments`** - Wraps positional and keyword arguments when a single item
   must expand into `fn(*args, **kwargs)`.

3. **Execution paths**:
   - `_simple_map` - Serial execution, optionally with a producer thread
     (`num_prepare > 0`).
   - `_concurrent_map` - One executor task per item/batch.
   - `_concurrent_chunk_map` - One executor task per chunk, processed sequentially
     inside the worker.

4. **`SupportsSubmit` / `FutureLike`** - Structural protocols describing the
   minimal executor and future interfaces.

## Technology Stack

- **Language**: Python 3.8+ (tested on 3.8-3.15, including free-threaded 3.13t-3.15t)
- **Package Manager**: [PDM](https://pdm.fming.dev/)
- **Build Backend**: `pdm-backend`
- **Task Runner**: [Nox](https://nox.thea.codes/)
- **Testing**: pytest, pytest-asyncio, pytest-cov, pytest-mock, pytest-timeout
- **Linting**: ruff (lint + format), mypy
- **CI/CD**: GitHub Actions

## Project Structure

```
fluentmap/
├── fluentmap.py         # Main module (single file, ~770 lines)
├── pyproject.toml       # Project configuration (PDM, tools config)
├── noxfile.py           # Task automation (format, test, type check)
├── pdm.lock             # Dependency lock file
├── tests/
│   ├── __init__.py
│   └── test_fluentmap.py
├── .github/workflows/
│   ├── test-suite.yml   # Run tests on push/PR
│   └── publish.yml      # Publish to PyPI on tag
├── AGENTS.md
├── README.md
└── LICENSE
```

## Build and Development Commands

### Setup

```bash
# Install PDM (if not already installed)
pip install pdm

# Install runtime and dev dependencies
pdm install -G dev
```

### Running Tests

```bash
# Run tests in the current environment (uses nox)
pdm run nox -s test

# Run tests in an isolated venv with test deps installed (CI path)
pdm run nox -s test_for_ci

# Run tests on every supported Python version (3.8-3.15)
pdm run nox -s test_all

# Run tests directly with pytest
pytest --cov=fluentmap.py --cov-report=term-missing tests
```

### Code Formatting

```bash
# Format code (ruff check --fix + ruff format)
pdm run nox -s format

# Check formatting without modifying files
pdm run nox -s format_check
```

### Type Checking

```bash
pdm run nox -s mypy
```

### Cleaning

```bash
pdm run nox -s clean
```

## Code Style Guidelines

### Line Length

- Maximum 88 characters (configured in `pyproject.toml` under `[tool.ruff]`).

### Lint Rules

- Ruff's `target-version` is inferred from `requires-python` (`py38`), and the
  same binary supports py38-py315.
- `ruff check --fix --unsafe-fixes` removes unused imports (`F401`) and unused
  variables (`F841`); `F401` is ignored in `__init__.py`.
- Duplicate dict keys (`F601`) are reported but have no autofix - fix manually.

### Python 3.8 Compatibility

- Keep source syntax valid for 3.8: no `match`, builtin generic annotations
  (`list[int]`), or `X | Y` unions.
- Use `typing` generics (`List`, `Dict`, `Optional`, `Union`).
- Nox-only code may use `tomllib` when available and fall back to `tomli` on
  Python < 3.11.

### Type Hints

- Public and private functions are fully annotated.
- Use `Protocol` + `runtime_checkable` for structural interfaces, and `cast`
  where static narrowing is impossible.
- TypeVars: `T` (result), `R` (transformed result), `T_co` (covariant future).

### Naming Conventions

- Public API: lowercase (`map`), classes in `PascalCase` (`Arguments`).
- Internal helpers and classes: leading underscore (`_simple_map`,
  `_concurrent_map`, `_process_chunk`).
- Constants: `UPPER_CASE` (`SOURCES`, `TEST_DEPS`).

### Documentation

- Module and public API docstrings use Sphinx-style fields (`:param:`, `:type:`,
  `:rtype:`); keep new parameters documented in the same format.
- Inline comments document non-obvious invariants (ordering, batching, cleanup).

## Testing Strategy

### Test Organization

- `tests/test_fluentmap.py` - All tests for the serial and executor paths.
- `tests/__init__.py` - Empty marker for the test package.
- Shared helper `map_func` adds optional sleeps and raised exceptions.

### Test Patterns

- Use `ThreadPoolExecutor` as a context manager and exercise both map paths with
  the same assertions.
- Timing-sensitive behavior (`num_prepare`, backpressure, early stop) is tested
  with short sleeps and tolerant assertions. Free-threaded builds must pass too.
- Use `pytest.raises` for exception propagation from tasks and `on_return`.

### Coverage

- Branch coverage is enabled in `[tool.coverage.run]` for `fluentmap.py`.
- Invoke `pytest --cov=fluentmap.py --cov-report=term-missing tests` for reports.

### Key Test Scenarios

- Empty/set/str/range iterables and multiple iterables of different lengths.
- `batch_size` batching (including incomplete final batch).
- `chunk_size` chunk submission and result ordering.
- `on_return` transformation and exception propagation.
- `Arguments` expansion and `sort_by_completion`.
- `num_prepare` backpressure and prompt termination on task failure.
- High-concurrency integrity (1024 tasks on 256 workers).

## CI/CD Pipeline

### Test Suite Workflow (`.github/workflows/test-suite.yml`)

- Triggers: push to `main`, pull requests, manual dispatch.
- Matrix: Python 3.8-3.15 plus free-threaded 3.13t, 3.14t, 3.15t on Windows,
  Ubuntu, and macOS.
- Steps: checkout, setup Python, setup PDM, `pdm add -d nox tomli`, then
  `pdm run nox -s test_for_ci`.
- `tomli` replaces `rtoml` because it is pure Python and has no native build for
  free-threaded interpreters.

### Publish Workflow (`.github/workflows/publish.yml`)

- Triggers: tag push (any tag pattern) and manual dispatch.
- Uses PyPI trusted publishing (OIDC) and creates a GitHub release.

## Version Management

- Version is defined in `fluentmap.py` as `__version__`.
- PDM reads it via `[tool.pdm.version]` (`source = "file"`).
- Current version: `0.1.0`.

## Dependencies

### Runtime Dependencies

- **None** - The library has zero runtime dependencies.

### Development Dependencies

- See `[dependency-groups.dev]` in `pyproject.toml`.
- `tomli` is only needed by `noxfile.py` on Python < 3.11.

## Security Considerations

1. **No External Runtime Dependencies**: Reduces supply chain attack surface.
2. **Trusted Publishing**: CI uses PyPI OIDC instead of API tokens.
3. **Thread Safety**: Shared state is exchanged through `Queue` and `Event`;
   result reordering happens in a single consumer thread.
4. **Bounded Work**: `num_prepare` limits in-flight items, preventing unbounded
   memory growth on large or unbounded iterables.

## Common Development Tasks

### Adding a New Feature

1. Implement in `fluentmap.py`, preserving builtin `map` semantics.
2. Add assertions to `test_simple_map` and `test_concurrent_map` (or a new test).
3. Run `pdm run nox -s format`.
4. Run `pdm run nox -s mypy`.
5. Run `pdm run nox -s test`.

### Adding a Python Version

1. Add the classifier in `pyproject.toml`.
2. Add the version to the CI matrix in `.github/workflows/test-suite.yml`.
3. Add the version to `test_all` in `noxfile.py`.
4. Update the supported versions in `README.md`.
5. Run `pdm lock` if development dependencies changed.

### Making a Release

1. Update `__version__` in `fluentmap.py`.
2. Commit and push the change.
3. Tag and push:
   ```bash
   git tag v0.1.0
   git push origin v0.1.0
   ```
4. The publish workflow uploads to PyPI and creates a GitHub release.

## Important Implementation Notes

1. **Generator Semantics**: `map()` validates arguments eagerly, then returns a
   lazy iterator. Multiple iterables follow `zip` semantics (shortest wins).

2. **Batch vs Chunk**:
   - `batch_size > 1` packs items into a list passed as one `fn` argument.
   - `chunk_size > 1` groups items into one executor task; items still invoke
     `fn` individually. Ignored when `executor` is `None`.

3. **Ordering**: Results default to submission order using cursor-based
   reordering. `sort_by_completion=True` yields as tasks finish.

4. **`on_return`**: Runs after each task. With an executor it executes in a
   dedicated helper thread; exceptions are re-raised from the iterator.

5. **Polling Loop**: Futures are polled (no blocking `wait`), with short
   `Queue.get(timeout=...)` calls to keep blocked workers interruptible.

6. **Compatibility**: Drop-in for builtin `map`; compatible with
   `asyncio.run_in_executor()` via any `submit`-based executor.
