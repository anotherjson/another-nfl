"""
Functional NFL Data CLI Tool with pure function architecture.

All CLI commands are implemented as pure functions returning CLIResult containers.
Command structure: extract → process → materialize → query
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import click
import pandas as pd
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

from .functional_utils import CLIResult, ExtractionConfig, QueryParams
from .functional_extraction import (
    extract_single_dataset,
    extract_multiple_years,
    extract_all_datasets,
    get_extraction_status,
    cleanup_old_extractions,
)
from .functional_processing import (
    process_parquet_file,
    scan_data_directory,
    validate_all_datasets,
    get_processing_summary,
)
from .functional_materialization import (
    materialize_by_priority,
    materialize_all_staging_models,
    materialize_specific_models,
    get_materialization_status,
    full_materialization_pipeline,
)
from .functional_querying import (
    query_staging_model,
    execute_custom_sql,
    list_all_models,
    get_model_summary,
    time_travel_query,
    check_query_system_health,
)

console = Console()


# Utility functions for CLI display
def display_result(result: CLIResult[Any], success_message: str = "Operation completed") -> None:
    """Display CLIResult with appropriate formatting."""
    if result.success:
        console.print(f"[green]✓ {success_message}[/green]")
        if result.data is not None:
            display_data(result.data)
    else:
        console.print(f"[red]✗ Operation failed: {result.error}[/red]")
        sys.exit(1)


def display_data(data: Any) -> None:
    """Display data with appropriate formatting based on type."""
    if isinstance(data, pd.DataFrame):
        if len(data) > 0:
            console.print("\n[cyan]Data Preview:[/cyan]")
            console.print(data.to_string())
        else:
            console.print("[yellow]No data to display[/yellow]")
    
    elif isinstance(data, dict):
        # Display dictionary as formatted table or JSON
        if "total_rows" in data or "row_count" in data:
            display_summary_table(data)
        else:
            import json
            console.print_json(json.dumps(data))
    
    elif isinstance(data, (list, tuple)):
        if len(data) > 0 and isinstance(data[0], dict):
            display_list_as_table(data)
        else:
            for item in data[:10]:  # Limit display
                console.print(f"  • {item}")
            if len(data) > 10:
                console.print(f"  ... and {len(data) - 10} more items")
    
    else:
        console.print(str(data))


def display_summary_table(data: dict[str, Any]) -> None:
    """Display summary data as a formatted table."""
    table = Table(title="Summary")
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")
    
    for key, value in data.items():
        if not isinstance(value, (dict, list)):
            display_value = str(value)
            if isinstance(value, float):
                display_value = f"{value:.2f}"
            table.add_row(key.replace('_', ' ').title(), display_value)
    
    console.print(table)


def display_list_as_table(data: list[dict[str, Any]]) -> None:
    """Display list of dictionaries as a table."""
    if not data:
        return
    
    # Get all unique keys
    all_keys = set()
    for item in data:
        all_keys.update(item.keys())
    
    table = Table()
    for key in sorted(all_keys):
        table.add_column(key.replace('_', ' ').title(), style="cyan")
    
    for item in data:
        row = []
        for key in sorted(all_keys):
            value = item.get(key, "")
            if isinstance(value, float):
                row.append(f"{value:.2f}")
            else:
                row.append(str(value))
        table.add_row(*row)
    
    console.print(table)


# Main CLI application
@click.group()
@click.version_option()
def nfl():
    """NFL Data CLI Tool - Functional Programming Edition."""
    pass


# Extract command group
@nfl.group()
def extract():
    """Extract NFL data from nfl_data_py."""
    pass


@extract.command("all")
@click.option("--year", type=int, required=True, help="Year to extract data for")
@click.option("--priority", type=click.Choice(['critical', 'high', 'medium', 'low']), 
              help="Extract only specific priority datasets")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed information")
def extract_all_command(year: int, priority: str | None, verbose: bool):
    """Extract all NFL datasets for a given year."""
    console.print(f"[blue]Extracting all datasets for year {year}[/blue]")
    if priority:
        console.print(f"[blue]Priority filter: {priority}[/blue]")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Extracting datasets...", total=None)
        
        result = extract_all_datasets(year, priority)
        
        progress.update(task, completed=1, total=1)
    
    if result.success and result.data:
        success_msg = f"Extracted {len(result.data)} datasets successfully"
        display_result(result, success_msg)
        
        if verbose:
            # Show detailed extraction information
            extraction_summary = {
                "total_datasets": len(result.data),
                "total_rows": sum(len(data[0]) for data in result.data.values()),
                "datasets_extracted": list(result.data.keys())
            }
            console.print("\n[cyan]Extraction Summary:[/cyan]")
            display_data(extraction_summary)
    else:
        display_result(result)


@extract.command("dataset")
@click.argument("dataset_name")
@click.option("--year", type=int, help="Year to extract (required for year-based datasets)")
@click.option("--validate/--no-validate", default=True, help="Validate extracted data")
@click.option("--save/--no-save", default=True, help="Save to disk")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed information")
def extract_dataset_command(dataset_name: str, year: int | None, validate: bool, save: bool, verbose: bool):
    """Extract a single NFL dataset."""
    console.print(f"[blue]Extracting dataset: {dataset_name}[/blue]")
    if year:
        console.print(f"[blue]Year: {year}[/blue]")
    
    config = ExtractionConfig(
        dataset_name=dataset_name,
        year=year,
        validate=validate,
        save_to_disk=save,
        output_path=None
    )
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Extracting data...", total=None)
        
        result = extract_single_dataset(config)
        
        progress.update(task, completed=1, total=1)
    
    if result.success and result.data:
        data, metadata = result.data
        console.print(f"\n[green]✓ Extraction completed successfully![/green]")
        
        # Display extraction metadata
        metadata_dict = {
            "rows_extracted": metadata.rows_extracted,
            "duration_seconds": metadata.duration_seconds,
            "validation_passed": metadata.validation_passed,
            "file_size_mb": metadata.file_size_mb,
            "output_path": str(metadata.output_path) if metadata.output_path else "Not saved"
        }
        display_data(metadata_dict)
        
        if verbose and len(data) > 0:
            console.print(f"\n[cyan]Data Preview (first 3 rows):[/cyan]")
            preview_data = data.head(3)
            console.print(preview_data.to_string())
    else:
        display_result(result)


@extract.command("status")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed status information")
def extract_status_command(verbose: bool):
    """Show extraction status and available datasets."""
    console.print("[blue]Getting extraction status...[/blue]")
    
    result = get_extraction_status()
    
    if result.success and result.data:
        status_data = result.data
        console.print(f"\n[green]✓ Extraction Status: {status_data['status']}[/green]")
        
        if status_data.get('datasets'):
            console.print(f"\n[cyan]Available Datasets ({status_data['total_datasets']}):[/cyan]")
            display_data(list(status_data['datasets'].items()))
        else:
            console.print("[yellow]No extracted datasets found[/yellow]")
    else:
        display_result(result)


@extract.command("cleanup")  
@click.option("--max-age-days", default=30, help="Maximum age in days for files to keep")
@click.option("--dry-run/--execute", default=True, help="Show what would be deleted without deleting")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed cleanup information")
def extract_cleanup_command(max_age_days: int, dry_run: bool, verbose: bool):
    """Clean up old extraction files."""
    action = "Scanning" if dry_run else "Cleaning up"
    console.print(f"[blue]{action} extraction files older than {max_age_days} days...[/blue]")
    
    result = cleanup_old_extractions(max_age_days, dry_run)
    
    if result.success and result.data:
        cleanup_data = result.data
        
        if dry_run:
            console.print(f"\n[yellow]Dry Run - Files that would be deleted:[/yellow]")
        else:
            console.print(f"\n[green]✓ Cleanup completed[/green]")
        
        summary = {
            "total_files": cleanup_data["total_files"],
            "total_size_mb": cleanup_data["total_size_mb"],
            "max_age_days": cleanup_data["max_age_days"]
        }
        
        if not dry_run and "deleted_count" in cleanup_data:
            summary["deleted_count"] = cleanup_data["deleted_count"]
        
        display_data(summary)
        
        if verbose and cleanup_data["files_to_delete"]:
            console.print("\n[cyan]Files:[/cyan]")
            display_data(cleanup_data["files_to_delete"])
    else:
        display_result(result)


# Process command group
@nfl.group()
def process():
    """Process and validate raw parquet files."""
    pass


@process.command("read")
@click.argument("file_path", type=click.Path(exists=True, path_type=Path))
@click.option("--limit", default=10, help="Number of rows to display")
@click.option("--validate/--no-validate", default=True, help="Validate data structure")
def process_read_command(file_path: Path, limit: int, validate: bool):
    """Read and inspect a parquet file."""
    console.print(f"[blue]Reading parquet file: {file_path}[/blue]")
    
    result = process_parquet_file(file_path, limit, validate)
    
    if result.success and result.data:
        file_data = result.data
        
        console.print(f"\n[green]✓ File processed successfully[/green]")
        
        # Display file information
        file_info = file_data["file_info"]
        schema_info = file_data["schema"]
        
        info_table = Table(title="File Information")
        info_table.add_column("Property", style="cyan")
        info_table.add_column("Value", style="magenta")
        
        info_table.add_row("File Size", f"{file_info['size_mb']} MB")
        info_table.add_row("Rows", str(schema_info["shape"][0]))
        info_table.add_row("Columns", str(schema_info["shape"][1]))
        info_table.add_row("Memory Usage", f"{schema_info['memory_usage_mb']} MB")
        info_table.add_row("Detected Type", file_data["detected_type"])
        info_table.add_row("Last Modified", file_info["modified_time"])
        
        console.print(info_table)
        
        # Display sample data
        sample_data = file_data["sample_data"]
        if len(sample_data) > 0:
            console.print(f"\n[cyan]Sample Data ({len(sample_data)} rows):[/cyan]")
            console.print(sample_data.to_string())
        
        # Display validation results if available
        if validate and file_data["validation_report"]:
            validation = file_data["validation_report"]
            console.print(f"\n[cyan]Validation Results:[/cyan]")
            console.print(f"✓ Total Rows: {validation.total_rows}")
            console.print(f"✓ Valid Rows: {validation.valid_rows}")
            console.print(f"✓ Validation Passed: {validation.validation_passed}")
            
            if validation.warnings:
                console.print("[yellow]Warnings:[/yellow]")
                for warning in validation.warnings:
                    console.print(f"  ⚠ {warning}")
            
            if validation.errors:
                console.print("[red]Errors:[/red]")
                for error in validation.errors:
                    console.print(f"  ✗ {error}")
    else:
        display_result(result)


@process.command("catalog")
@click.option("--base-path", type=click.Path(path_type=Path), default=Path("data"), 
              help="Base data directory to scan")
def process_catalog_command(base_path: Path):
    """Show catalog of all available datasets."""
    console.print(f"[blue]Scanning data directory: {base_path}[/blue]")
    
    result = scan_data_directory(base_path)
    
    if result.success and result.data:
        dataset_infos = result.data
        console.print(f"\n[green]✓ Found {len(dataset_infos)} datasets[/green]")
        
        if dataset_infos:
            catalog_table = Table(title="Dataset Catalog")
            catalog_table.add_column("Dataset", style="cyan")
            catalog_table.add_column("Rows", style="magenta", justify="right")
            catalog_table.add_column("Size (MB)", style="green", justify="right")
            catalog_table.add_column("Last Modified", style="yellow")
            
            for info in dataset_infos:
                catalog_table.add_row(
                    info.name,
                    str(info.row_count),
                    str(info.file_size_mb),
                    info.last_modified.split('T')[0]  # Show just the date
                )
            
            console.print(catalog_table)
        else:
            console.print("[yellow]No datasets found[/yellow]")
    else:
        display_result(result)


@process.command("validate")
@click.option("--base-path", type=click.Path(path_type=Path), default=Path("data"), 
              help="Base data directory to validate")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed validation results")
def process_validate_command(base_path: Path, verbose: bool):
    """Validate all datasets in data directory."""
    console.print(f"[blue]Validating all datasets in: {base_path}[/blue]")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Validating datasets...", total=None)
        
        result = validate_all_datasets(base_path)
        
        progress.update(task, completed=1, total=1)
    
    if result.success and result.data:
        validations = result.data
        console.print(f"\n[green]✓ Validated {len(validations)} datasets[/green]")
        
        # Summary statistics
        passed = sum(1 for v in validations.values() if v.validation_passed)
        failed = len(validations) - passed
        total_warnings = sum(len(v.warnings) for v in validations.values())
        total_errors = sum(len(v.errors) for v in validations.values())
        
        summary_table = Table(title="Validation Summary")
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Count", style="magenta", justify="right")
        
        summary_table.add_row("Total Datasets", str(len(validations)))
        summary_table.add_row("Passed Validation", str(passed))
        summary_table.add_row("Failed Validation", str(failed))
        summary_table.add_row("Total Warnings", str(total_warnings))
        summary_table.add_row("Total Errors", str(total_errors))
        
        console.print(summary_table)
        
        if verbose:
            # Show detailed results for each dataset
            for dataset_name, validation in validations.items():
                status = "✓ PASS" if validation.validation_passed else "✗ FAIL"
                console.print(f"\n[cyan]{dataset_name}:[/cyan] {status}")
                console.print(f"  Rows: {validation.total_rows} (valid: {validation.valid_rows})")
                
                if validation.warnings:
                    for warning in validation.warnings:
                        console.print(f"  [yellow]⚠ {warning}[/yellow]")
                
                if validation.errors:
                    for error in validation.errors:
                        console.print(f"  [red]✗ {error}[/red]")
    else:
        display_result(result)


# Materialize command group
@nfl.group() 
def materialize():
    """Materialize staging models via Dagster and dbt."""
    pass


@materialize.command("all")
@click.option("--tests/--no-tests", default=True, help="Run tests after materialization")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed materialization information")
def materialize_all_command(tests: bool, verbose: bool):
    """Materialize all staging models."""
    console.print("[blue]Materializing all staging models...[/blue]")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Running materialization pipeline...", total=None)
        
        result = materialize_all_staging_models(tests)
        
        progress.update(task, completed=1, total=1)
    
    if result.success and result.data:
        materialization = result.data
        console.print(f"\n[green]✓ Materialization completed[/green]")
        
        # Display results
        results_table = Table(title="Materialization Results")
        results_table.add_column("Metric", style="cyan")
        results_table.add_column("Value", style="magenta")
        
        results_table.add_row("Models Materialized", str(len(materialization.models_materialized)))
        results_table.add_row("Successful Operations", str(materialization.success_count))
        results_table.add_row("Errors", str(materialization.error_count))
        results_table.add_row("Duration", f"{materialization.total_duration_seconds}s")
        
        console.print(results_table)
        
        if materialization.models_materialized:
            console.print(f"\n[cyan]Materialized Models:[/cyan]")
            for model in materialization.models_materialized:
                console.print(f"  ✓ {model}")
        
        if materialization.errors and verbose:
            console.print(f"\n[red]Errors:[/red]")
            for error in materialization.errors:
                console.print(f"  ✗ {error}")
    else:
        display_result(result)


@materialize.command("staging")
@click.option("--priority", type=click.Choice(['critical', 'high', 'medium', 'low']), 
              required=True, help="Priority level to materialize")
@click.option("--tests/--no-tests", default=True, help="Run tests after materialization")
def materialize_staging_command(priority: str, tests: bool):
    """Materialize staging models by priority."""
    console.print(f"[blue]Materializing {priority} priority staging models...[/blue]")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task(f"Materializing {priority} models...", total=None)
        
        result = materialize_by_priority(priority, tests)
        
        progress.update(task, completed=1, total=1)
    
    if result.success and result.data:
        materialization = result.data
        success_msg = f"Materialized {len(materialization.models_materialized)} {priority} priority models"
        display_result(result, success_msg)
        
        # Show materialized models
        if materialization.models_materialized:
            console.print(f"\n[cyan]Materialized Models:[/cyan]")
            for model in materialization.models_materialized:
                console.print(f"  ✓ {model}")
    else:
        display_result(result)


@materialize.command("models")
@click.argument("model_names", nargs=-1, required=True)
@click.option("--tests/--no-tests", default=True, help="Run tests after materialization")  
def materialize_models_command(model_names: tuple[str, ...], tests: bool):
    """Materialize specific models by name."""
    model_list = list(model_names)
    console.print(f"[blue]Materializing models: {', '.join(model_list)}[/blue]")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Materializing models...", total=None)
        
        result = materialize_specific_models(model_list, tests)
        
        progress.update(task, completed=1, total=1)
    
    display_result(result, f"Materialized {len(model_list)} models successfully")


@materialize.command("status")
def materialize_status_command():
    """Show materialization status."""
    console.print("[blue]Getting materialization status...[/blue]")
    
    result = get_materialization_status()
    
    if result.success and result.data:
        status_data = result.data
        console.print(f"\n[green]✓ Materialization Status Retrieved[/green]")
        
        # Display summary
        summary = status_data.get("summary", {})
        if summary:
            console.print(f"\n[cyan]System Summary:[/cyan]")
            display_data(summary)
        
        # Display dbt models information
        dbt_info = status_data.get("dbt_models", {})
        if dbt_info and "total_models" in dbt_info:
            console.print(f"\n[cyan]dbt Models:[/cyan]")
            console.print(f"  Total Models: {dbt_info['total_models']}")
            console.print(f"  Staging Models: {len(dbt_info.get('staging_models', []))}")
            console.print(f"  Intermediate Models: {len(dbt_info.get('intermediate_models', []))}")
        
        # Display Dagster assets information
        dagster_info = status_data.get("dagster_assets", {})
        if dagster_info and "total_assets" in dagster_info:
            console.print(f"\n[cyan]Dagster Assets:[/cyan]")
            console.print(f"  Total Assets: {dagster_info['total_assets']}")
            console.print(f"  Materialized: {dagster_info.get('materialized_assets', 0)}")
            console.print(f"  Never Materialized: {dagster_info.get('never_materialized', 0)}")
    else:
        display_result(result)


# Query command group
@nfl.group()
def query():
    """Query staging models via DuckLake."""
    pass


@query.command("staging")
@click.argument("model_name")
@click.option("--limit", type=int, help="Number of rows to return")
@click.option("--as-of-date", help="Query data as of specific date (YYYY-MM-DD)")
@click.option("--show-schema", is_flag=True, help="Show model schema information")
def query_staging_command(model_name: str, limit: int | None, as_of_date: str | None, show_schema: bool):
    """Query a staging model."""
    console.print(f"[blue]Querying staging model: {model_name}[/blue]")
    if as_of_date:
        console.print(f"[blue]As of date: {as_of_date}[/blue]")
    
    params = QueryParams(
        model_name=model_name,
        limit=limit,
        as_of_date=as_of_date,
        show_schema=show_schema,
        filters={}
    )
    
    result = query_staging_model(params)
    
    if result.success and result.data:
        query_result = result.data
        console.print(f"\n[green]✓ Query executed successfully[/green]")
        
        # Display query metadata
        metadata_table = Table(title="Query Results")
        metadata_table.add_column("Metric", style="cyan")
        metadata_table.add_column("Value", style="magenta")
        
        metadata_table.add_row("Rows Returned", str(query_result.row_count))
        metadata_table.add_row("Columns", str(query_result.column_count))
        metadata_table.add_row("Execution Time", f"{query_result.execution_time_ms}ms")
        
        console.print(metadata_table)
        
        # Display data
        if len(query_result.data) > 0:
            console.print(f"\n[cyan]Query Results:[/cyan]")
            console.print(query_result.data.to_string())
        else:
            console.print("[yellow]No data returned[/yellow]")
        
        if show_schema:
            console.print(f"\n[cyan]SQL Query:[/cyan]")
            console.print(query_result.query_sql)
    else:
        display_result(result)


@query.command("sql")
@click.argument("sql_query")
def query_sql_command(sql_query: str):
    """Execute custom SQL query."""
    console.print("[blue]Executing custom SQL query...[/blue]")
    
    result = execute_custom_sql(sql_query)
    
    if result.success and result.data:
        query_result = result.data
        console.print(f"\n[green]✓ SQL executed successfully[/green]")
        
        # Display execution info
        console.print(f"Rows: {query_result.row_count}, Columns: {query_result.column_count}")
        console.print(f"Execution time: {query_result.execution_time_ms}ms")
        
        # Display results
        if len(query_result.data) > 0:
            console.print(f"\n[cyan]Results:[/cyan]")
            console.print(query_result.data.to_string())
        else:
            console.print("[yellow]No data returned[/yellow]")
    else:
        display_result(result)


@query.command("models")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed model information")
def query_models_command(verbose: bool):
    """List all available staging models."""
    console.print("[blue]Getting available models...[/blue]")
    
    result = list_all_models()
    
    if result.success and result.data:
        models_data = result.data
        console.print(f"\n[green]✓ Found {models_data['total_models']} models[/green]")
        
        # Display model counts by type
        summary_table = Table(title="Model Summary")
        summary_table.add_column("Type", style="cyan")
        summary_table.add_column("Count", style="magenta", justify="right")
        
        summary_table.add_row("Total Models", str(models_data["total_models"]))
        summary_table.add_row("Staging Models", str(len(models_data["staging_models"])))
        summary_table.add_row("Intermediate Models", str(len(models_data["intermediate_models"])))
        summary_table.add_row("Mart Models", str(len(models_data["mart_models"])))
        
        console.print(summary_table)
        
        # Display staging models
        if models_data["staging_models"]:
            console.print(f"\n[cyan]Staging Models:[/cyan]")
            for model in models_data["staging_models"]:
                console.print(f"  • {model}")
        
        if verbose and models_data.get("model_details"):
            console.print(f"\n[cyan]Model Details:[/cyan]")
            details_table = Table()
            details_table.add_column("Model", style="cyan")
            details_table.add_column("Type", style="yellow")
            details_table.add_column("Rows", style="magenta", justify="right")
            
            for model_name, details in models_data["model_details"].items():
                details_table.add_row(
                    model_name,
                    details.get("type", "unknown"),
                    str(details.get("row_count", "N/A"))
                )
            
            console.print(details_table)
    else:
        display_result(result)


@query.command("schema")
@click.argument("model_name")
def query_schema_command(model_name: str):
    """Show schema information for a model."""
    console.print(f"[blue]Getting schema for model: {model_name}[/blue]")
    
    result = get_model_summary(model_name)
    
    if result.success and result.data:
        summary = result.data
        console.print(f"\n[green]✓ Schema retrieved for {model_name}[/green]")
        
        # Display basic info
        console.print(f"Rows: {summary['row_count']}")
        console.print(f"Columns: {summary['schema']['total_columns']}")
        
        # Display column information
        console.print(f"\n[cyan]Columns:[/cyan]")
        schema_table = Table()
        schema_table.add_column("Column", style="cyan")
        schema_table.add_column("Type", style="yellow")
        schema_table.add_column("Not Null", style="magenta")
        
        for column in summary["schema"]["columns"]:
            schema_table.add_row(
                column["name"],
                column["type"],
                "Yes" if column["not_null"] else "No"
            )
        
        console.print(schema_table)
        
        # Display sample data
        sample_data = summary["sample_data"]
        if len(sample_data) > 0:
            console.print(f"\n[cyan]Sample Data:[/cyan]")
            console.print(sample_data.to_string())
    else:
        display_result(result)


# Health check command
@nfl.command("health")
def health_command():
    """Check health of all system components."""
    console.print("[blue]Checking system health...[/blue]")
    
    # Check query system
    query_health = check_query_system_health()
    
    # Get extraction status  
    extraction_status = get_extraction_status()
    
    # Get materialization status
    materialization_status = get_materialization_status()
    
    # Display overall health
    console.print(f"\n[cyan]System Health Check - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/cyan]")
    
    health_table = Table(title="Component Health")
    health_table.add_column("Component", style="cyan")
    health_table.add_column("Status", style="magenta")
    health_table.add_column("Details", style="yellow")
    
    # Query system health
    if query_health.success and query_health.data:
        query_data = query_health.data
        status = "✓ Healthy" if query_data["overall_healthy"] else "✗ Issues"
        details = f"{len(query_data['components'])} components checked"
        health_table.add_row("Query System", status, details)
    else:
        health_table.add_row("Query System", "✗ Failed", query_health.error or "Unknown error")
    
    # Extraction system health
    if extraction_status.success and extraction_status.data:
        ext_data = extraction_status.data
        status = "✓ Healthy" if ext_data["status"] == "active" else "⚠ Empty"
        details = f"{ext_data.get('total_datasets', 0)} datasets available"
        health_table.add_row("Extraction System", status, details)
    else:
        health_table.add_row("Extraction System", "✗ Failed", extraction_status.error or "Unknown error")
    
    # Materialization system health
    if materialization_status.success and materialization_status.data:
        mat_data = materialization_status.data
        summary = mat_data.get("summary", {})
        health_status = "✓ Healthy" if summary.get("overall_health") == "healthy" else "⚠ Needs attention"
        details = f"{summary.get('dbt_models_available', 0)} dbt models, {summary.get('dagster_assets_available', 0)} assets"
        health_table.add_row("Materialization System", health_status, details)
    else:
        health_table.add_row("Materialization System", "✗ Failed", materialization_status.error or "Unknown error")
    
    console.print(health_table)


if __name__ == "__main__":
    nfl()