# Repository Guidelines

## Project Structure & Module Organization
- `jqsys/`: Installable package with auth, client, and analytics modules; keep reusable logic here with type hints and docstrings.
- `scripts/`: CLI entry points (e.g., `python scripts/jq_auth.py --help`) that should import from `jqsys` rather than duplicating logic.
- `tests/`: Pytest suites mirrored to package layout (`tests/auth/`, `tests/storage/`, etc.); add new test files as `test_*.py`.
- `docs/` and `AGENTS.md`: Living documentation—update whenever behavior or workflows change.
- `data/` and `notebooks/`: Local artifacts and exploratory work; do not rely on them for automated flows.
- `jquants_api_quick_start_en.py`: Reference-only tutorial; never import it or execute it in CI paths.

## Build, Test, and Development Commands
- Create/activate env: `source .venv/bin/activate` (managed by `uv`).
- Install editable package: `uv pip install -e .`; add optional dev tools via `uv pip install -e .[dev]`.
- Bootstrap hooks: `./setup-hooks.sh` to install the pre-commit runner that executes tests before every commit.
- Run verification locally: `python run_tests.py` (wrapper) or `pytest tests -v` for targeted runs.
- Execute utilities: `python scripts/<name>.py [flags]` after the editable install.

## Coding Style & Naming Conventions
- Follow PEP 8 with 4-space indentation, descriptive snake_case for functions/modules, and UpperCamelCase for classes.
- Use type annotations and concise docstrings, matching existing patterns in `jqsys/auth.py`.
- Keep functions focused and side-effect free; push configuration into `jqsys/utils` where possible.

## Testing Guidelines
- Prefer pytest fixtures and parametrization; place shared helpers in `tests/conftest.py`.
- New functionality requires unit tests plus regression coverage for edge cases; mimic existing naming (`test_<feature>_...`).
- Ensure `python run_tests.py` passes before pushing—CI mirrors this command via the pre-commit hook.

## Commit & Pull Request Guidelines
- Use Conventional Commit prefixes observed in history (`feat:`, `fix:`, `chore:`, `test:`, `perf:`) with imperative summaries under 72 characters.
- Squash small fixups locally; PRs should describe scope, testing performed, and any follow-up tasks.
- Link issues or plan items (see `docs/project_plan.md`), and include CLI output or screenshots when behavior changes.
- Verify hooks still run post-change; if they fail, document the remediation in the PR description.

## Security & Configuration Tips
- Store API tokens in `.env` (`JQ_REFRESH_TOKEN` demo access until 2025‑05‑31). Load via `jqsys.utils.env.load_env_file_if_present` instead of hardcoding secrets.
- Never commit `.env`, data extracts, or credentials; prefer environment variables (`JQSYS_DATA_ROOT`) for local paths.
- Review `docs/jquants_cli_usage.md` for approved API flows before adding new endpoints or scopes.
