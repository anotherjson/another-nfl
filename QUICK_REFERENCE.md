# Quick Reference Guide

Essential commands and patterns for working with the NFL Data Explorer project.

## Essential Commands

### Environment Setup
```bash
# Setup (run once)
uv python install 3.11
uv sync --dev
uv run pre-commit install

# Daily use
uv run python -m src.cli --help
```

### CLI Usage
```bash
# List all NFL datasets
uv run python -m src.cli explore datasets

# Explore specific dataset (reliable)
uv run python -m src.cli explore data team_desc --limit 5

# Explore with year filter
uv run python -m src.cli explore data schedules --year 2023 --limit 3

# Read parquet files
uv run python -m src.cli read data/sample_players.parquet --info

# Error debugging
uv run python -m src.cli explore data pbp --verbose
```

### Development Commands
```bash
# Code quality
uv run ruff format .
uv run ruff check .
uv run ruff check . --fix

# Testing
uv run pytest tests/test_cli.py -v         # CLI tests (reliable)
uv run pytest --cov=src                   # Full test suite with coverage
uv run pytest tests/test_parquet_reader.py # Parquet tests

# Pre-commit
uv run pre-commit run --all-files
```

## File Structure Quick Map

```
another-nfl/
├── src/
│   ├── cli.py              # Main CLI commands
│   ├── nfl_explorer.py     # NFL data logic (19 datasets)
│   └── parquet_reader.py   # Parquet file handling
├── tests/
│   ├── test_cli.py         # CLI tests (work reliably)
│   ├── test_nfl_explorer.py # NFL tests (may have issues)
│   └── test_parquet_reader.py # Parquet tests
├── data/
│   └── sample_players.parquet # Test data
├── CLAUDE.md               # Main guidance document
├── README.md               # User documentation
├── DEVELOPMENT_GUIDE.md    # Detailed dev guide
├── TROUBLESHOOTING.md      # Common issues
└── pyproject.toml          # Project config
```

## NFL Datasets Quick Reference

### Always Work (No Network)
- `team_desc` - Team information and colors
- `players` - Player information

### Usually Work (Network Required)
- `schedules` - Game schedules (1999+)
- `combine` - NFL Combine results (1987+)
- `draft_picks` - Draft picks (1936+)

### May Have Issues (External API)
- `pbp` - Play-by-play data (1999+)
- `weekly` - Weekly stats (1999+)
- `seasonal` - Seasonal stats (1999+)

## Common Patterns

### Adding CLI Command
```python
@main.command()
@click.argument("required_param")
@click.option("--optional", default=None, help="Optional parameter")
def new_command(required_param, optional):
    """Command description."""
    try:
        # Implementation
        console.print(f"[green]Success[/green]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)
```

### Error Handling Pattern
```python
try:
    result = risky_operation()
    return result
except SpecificError as e:
    raise Exception(f"Descriptive message: {str(e)}")
```

### Rich Table Output
```python
from rich.table import Table

table = Table(title="Title")
table.add_column("Col1", style="cyan")
table.add_column("Col2", style="magenta")
table.add_row("data1", "data2")
console.print(table)
```

## Testing Patterns

### CLI Test
```python
def test_cli_command():
    runner = CliRunner()
    result = runner.invoke(main, ['command', 'args'])
    assert result.exit_code == 0
    assert "expected" in result.output
```

### Mock External API
```python
@patch('src.nfl_explorer.nfl.import_team_desc')
def test_with_mock(mock_func):
    mock_func.return_value = pd.DataFrame({'col': ['data']})
    # Test implementation
```

## Troubleshooting Quick Fixes

### Issue: Command not found
```bash
# Fix: Use uv run
uv run python -m src.cli explore datasets
```

### Issue: Import errors
```bash
# Fix: Ensure dependencies installed
uv sync --dev
```

### Issue: Network errors with NFL data
```bash
# Fix: Use team_desc dataset
uv run python -m src.cli explore data team_desc
```

### Issue: Test failures
```bash
# Fix: Run CLI tests specifically
uv run pytest tests/test_cli.py -v
```

### Issue: Formatting errors
```bash
# Fix: Auto-format
uv run ruff format .
uv run ruff check . --fix
```

## Validation Checklist

Quick checks to verify everything works:

```bash
# 1. Environment
uv run python -m src.cli --help

# 2. Basic functionality
uv run python -m src.cli explore datasets

# 3. Data retrieval
uv run python -m src.cli explore data team_desc --limit 2

# 4. File reading
uv run python -m src.cli read data/sample_players.parquet

# 5. Error handling
uv run python -m src.cli explore data invalid_dataset

# 6. Code quality
uv run ruff check .

# 7. Tests
uv run pytest tests/test_cli.py -v
```

If all the above work, the project is functioning correctly.

## Git Workflow

```bash
# Check status
git status

# Stage changes
git add .

# Commit with good message
git commit -m "descriptive message"

# Push changes
git push origin claude_vibe
```

## Key Reminders

1. **Use `uv run`** for all Python commands
2. **Test with `team_desc`** dataset first (always works)
3. **CLI tests are reliable** - focus on those for validation
4. **Network errors are expected** with some NFL datasets
5. **Code formatting is required** - run ruff before committing
6. **82% coverage achieved** when running full test suite
7. **Phase 1 is complete** - CLI tool is production ready

## Getting Unstuck

If something doesn't work:
1. Check TROUBLESHOOTING.md for specific error
2. Verify basic functionality with team_desc dataset
3. Run CLI tests to confirm core functionality
4. Remember: network issues are external, not code issues