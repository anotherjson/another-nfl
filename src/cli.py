"""NFL Data CLI Tool for exploration and debugging."""

import sys
from pathlib import Path

import click
import pandas as pd
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from src.extraction_manager import ExtractionManager
from src.nfl_explorer import NFLExplorer
from src.nfl_extractor import NFLDataExtractor
from src.parquet_reader import ParquetReader
from src.ducklake_manager import DuckLakeManager

console = Console()


@click.group()
@click.version_option()
def main():
    """NFL Data CLI tool for exploring NFL datasets and reading parquet files."""
    pass


@main.group()
def explore():
    """Explore NFL datasets using nfl_data_py."""
    pass


@explore.command("datasets")
def explore_datasets():
    """List all available NFL datasets from nfl_data_py."""
    try:
        explorer = NFLExplorer()
        datasets = explorer.list_datasets()

        table = Table(title="Available NFL Datasets")
        table.add_column("Dataset", style="cyan", no_wrap=True)
        table.add_column("Description", style="magenta")
        table.add_column("Start Year", style="green", justify="center")

        for dataset in datasets:
            table.add_row(
                dataset["name"],
                dataset["description"],
                str(dataset["start_year"]) if dataset["start_year"] else "N/A",
            )

        console.print(table)

    except Exception as e:
        console.print(f"[red]Error listing datasets: {e}[/red]")
        sys.exit(1)


@explore.command("data")
@click.argument("dataset")
@click.option("--year", type=int, help="Year to fetch data for")
@click.option("--limit", default=5, help="Number of rows to display (default: 5)")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed error information")
def explore_data(dataset: str, year: int | None, limit: int, verbose: bool):
    """Show sample data from a specific NFL dataset."""
    try:
        explorer = NFLExplorer()

        if not explorer.is_valid_dataset(dataset):
            available = [d["name"] for d in explorer.list_datasets()]
            console.print(f"[red]Invalid dataset: {dataset}[/red]")
            console.print(
                f"[yellow]Available datasets: {', '.join(available)}[/yellow]"
            )
            sys.exit(1)

        console.print(f"[blue]Fetching data for dataset: {dataset}[/blue]")
        if year:
            console.print(f"[blue]Year: {year}[/blue]")

        data = explorer.get_sample_data(dataset, year=year, limit=limit)

        if data.empty:
            console.print(f"[yellow]No data found for {dataset}[/yellow]")
            return

        console.print(f"\n[green]Sample data ({len(data)} rows):[/green]")
        console.print(data.to_string())

        console.print(f"\n[cyan]Data shape: {data.shape}[/cyan]")
        console.print(f"[cyan]Columns: {list(data.columns)}[/cyan]")

    except Exception as e:
        if verbose:
            console.print(f"[red]Detailed error: {type(e).__name__}: {e}[/red]")
            import traceback

            console.print(f"[red]Traceback:\n{traceback.format_exc()}[/red]")
        else:
            console.print(f"[red]Error fetching data: {e}[/red]")
            console.print(
                "[yellow]Use --verbose for detailed error information[/yellow]"
            )
        sys.exit(1)


@main.group()
def extract():
    """Production data extraction commands."""
    pass


@extract.command("dataset")
@click.argument("dataset_name")
@click.option(
    "--year", type=int, help="Year to extract (required for year-based datasets)"
)
@click.option(
    "--validate",
    is_flag=True,
    default=True,
    help="Validate extracted data (default: True)",
)
@click.option("--save", is_flag=True, default=True, help="Save to disk (default: True)")
@click.option(
    "--verbose", "-v", is_flag=True, help="Show detailed extraction information"
)
def extract_dataset(
    dataset_name: str, year: int | None, validate: bool, save: bool, verbose: bool
):
    """Extract a single dataset with full production capabilities."""
    try:
        extractor = NFLDataExtractor()

        # Get dataset info
        try:
            info = extractor.get_dataset_info(dataset_name)
        except KeyError:
            console.print(f"[red]Invalid dataset: {dataset_name}[/red]")
            available = extractor.list_available_datasets()
            console.print(
                f"[yellow]Available datasets: {', '.join(available)}[/yellow]"
            )
            sys.exit(1)

        # Check year requirement
        if info["requires_year"] and year is None:
            console.print(
                f"[red]Dataset '{dataset_name}' requires --year parameter[/red]"
            )
            console.print(
                f"[yellow]Dataset starts from year: {info['start_year']}[/yellow]"
            )
            sys.exit(1)

        console.print(f"[blue]Extracting dataset: {dataset_name}[/blue]")
        if year:
            console.print(f"[blue]Year: {year}[/blue]")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Extracting data...", total=None)

            try:
                data, metadata = extractor.extract_dataset(
                    dataset_name=dataset_name,
                    year=year,
                    validate=validate,
                    save_to_disk=save,
                )

                progress.update(task, completed=1, total=1)

            except Exception as e:
                progress.stop()
                if verbose:
                    console.print(f"[red]Detailed error: {type(e).__name__}: {e}[/red]")
                    import traceback

                    console.print(f"[red]Traceback:\n{traceback.format_exc()}[/red]")
                else:
                    console.print(f"[red]Extraction failed: {e}[/red]")
                    console.print(
                        "[yellow]Use --verbose for detailed error information[/yellow]"
                    )
                sys.exit(1)

        # Display results
        console.print("\n[green]✓ Extraction completed successfully![/green]")

        # Create results table
        results_table = Table(title="Extraction Results")
        results_table.add_column("Metric", style="cyan")
        results_table.add_column("Value", style="magenta")

        results_table.add_row("Rows Extracted", str(metadata["rows_extracted"]))
        results_table.add_row("Duration", f"{metadata['duration_seconds']}s")
        results_table.add_row(
            "Validation", "✓ Passed" if metadata["validation_passed"] else "✗ Failed"
        )
        if metadata.get("output_path"):
            results_table.add_row("Output File", str(metadata["output_path"]))
            results_table.add_row(
                "File Size", f"{metadata.get('file_size_mb', 0):.2f} MB"
            )

        console.print(results_table)

        if verbose and data is not None and not data.empty:
            console.print(
                f"\n[cyan]Preview of extracted data ({min(3, len(data))} rows):[/cyan]"
            )
            console.print(data.head(3).to_string())

    except Exception as e:
        console.print(f"[red]Extraction error: {e}[/red]")
        sys.exit(1)


@extract.command("multiple")
@click.argument("dataset_name")
@click.option(
    "--years",
    required=True,
    help="Comma-separated years to extract (e.g., 2020,2021,2022)",
)
@click.option(
    "--validate",
    is_flag=True,
    default=True,
    help="Validate extracted data (default: True)",
)
@click.option("--save", is_flag=True, default=True, help="Save to disk (default: True)")
@click.option(
    "--verbose", "-v", is_flag=True, help="Show detailed extraction information"
)
def extract_multiple_years(
    dataset_name: str, years: str, validate: bool, save: bool, verbose: bool
):
    """Extract multiple years of a dataset."""
    try:
        extractor = NFLDataExtractor()

        # Parse years
        try:
            year_list = [int(y.strip()) for y in years.split(",")]
        except ValueError:
            console.print(f"[red]Invalid years format: {years}[/red]")
            console.print(
                "[yellow]Use comma-separated format like: 2020,2021,2022[/yellow]"
            )
            sys.exit(1)

        # Get dataset info
        try:
            info = extractor.get_dataset_info(dataset_name)
        except KeyError:
            console.print(f"[red]Invalid dataset: {dataset_name}[/red]")
            available = extractor.list_available_datasets()
            console.print(
                f"[yellow]Available datasets: {', '.join(available)}[/yellow]"
            )
            sys.exit(1)

        if not info["requires_year"]:
            console.print(
                f"[red]Dataset '{dataset_name}' doesn't support year-based extraction[/red]"
            )
            sys.exit(1)

        console.print(f"[blue]Extracting dataset: {dataset_name}[/blue]")
        console.print(f"[blue]Years: {', '.join(map(str, year_list))}[/blue]")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Extracting years...", total=len(year_list))

            try:
                results = extractor.extract_multiple_years(
                    dataset_name=dataset_name,
                    years=year_list,
                    validate=validate,
                    save_to_disk=save,
                )

                progress.update(task, completed=len(year_list))

            except Exception as e:
                progress.stop()
                if verbose:
                    console.print(f"[red]Detailed error: {type(e).__name__}: {e}[/red]")
                    import traceback

                    console.print(f"[red]Traceback:\n{traceback.format_exc()}[/red]")
                else:
                    console.print(f"[red]Extraction failed: {e}[/red]")
                    console.print(
                        "[yellow]Use --verbose for detailed error information[/yellow]"
                    )
                sys.exit(1)

        # Display results
        console.print("\n[green]✓ Multi-year extraction completed![/green]")

        # Create results table
        results_table = Table(title="Multi-Year Extraction Results")
        results_table.add_column("Year", style="cyan")
        results_table.add_column("Status", style="magenta")
        results_table.add_column("Rows", style="green", justify="right")
        results_table.add_column("Size (MB)", style="blue", justify="right")

        total_rows = 0
        successful_years = 0

        for year in year_list:
            if year in results:
                data, metadata = results[year]
                if "error" not in metadata:
                    status = "✓ Success"
                    rows = metadata["rows_extracted"]
                    size = metadata.get("file_size_mb", 0)
                    total_rows += rows
                    successful_years += 1
                else:
                    status = "✗ Failed"
                    rows = 0
                    size = 0

                results_table.add_row(str(year), status, str(rows), f"{size:.2f}")

        console.print(results_table)

        # Summary
        console.print(
            f"\n[cyan]Summary: {successful_years}/{len(year_list)} years successful, {total_rows:,} total rows extracted[/cyan]"
        )

    except Exception as e:
        console.print(f"[red]Multi-year extraction error: {e}[/red]")
        sys.exit(1)


@extract.command("incremental")
@click.argument("dataset_name")
@click.option(
    "--years",
    help="Comma-separated years to consider (optional for year-based datasets)",
)
@click.option(
    "--max-age-days",
    default=1,
    type=int,
    help="Maximum age in days to consider current (default: 1)",
)
@click.option("--force", is_flag=True, help="Force refresh even if data is current")
@click.option(
    "--verbose", "-v", is_flag=True, help="Show detailed extraction information"
)
def extract_incremental(
    dataset_name: str,
    years: str | None,
    max_age_days: int,
    force: bool,
    verbose: bool,
):
    """Perform incremental extraction (skips already-current data)."""
    try:
        manager = ExtractionManager()

        # Parse years if provided
        year_list = None
        if years:
            try:
                year_list = [int(y.strip()) for y in years.split(",")]
            except ValueError:
                console.print(f"[red]Invalid years format: {years}[/red]")
                console.print(
                    "[yellow]Use comma-separated format like: 2020,2021,2022[/yellow]"
                )
                sys.exit(1)
        else:
            # Get recommended years for year-based datasets
            recommended = manager.get_recommended_years(dataset_name, limit=3)
            if recommended:
                year_list = recommended
                console.print(
                    f"[blue]Using recommended years: {', '.join(map(str, year_list))}[/blue]"
                )

        console.print(
            f"[blue]Incremental extraction for dataset: {dataset_name}[/blue]"
        )
        console.print(f"[blue]Max age: {max_age_days} days[/blue]")
        if force:
            console.print(
                "[yellow]Force refresh enabled - will re-extract all data[/yellow]"
            )

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Processing incremental extraction...", total=None)

            try:
                summary = manager.extract_incremental(
                    dataset_name=dataset_name,
                    years=year_list,
                    force_refresh=force,
                    max_age_days=max_age_days,
                )

                progress.update(task, completed=1, total=1)

            except Exception as e:
                progress.stop()
                if verbose:
                    console.print(f"[red]Detailed error: {type(e).__name__}: {e}[/red]")
                    import traceback

                    console.print(f"[red]Traceback:\n{traceback.format_exc()}[/red]")
                else:
                    console.print(f"[red]Incremental extraction failed: {e}[/red]")
                    console.print(
                        "[yellow]Use --verbose for detailed error information[/yellow]"
                    )
                sys.exit(1)

        # Display results
        status_color = "green" if summary["success"] else "yellow"
        status_text = (
            "✓ Completed successfully"
            if summary["success"]
            else "⚠ Completed with issues"
        )
        console.print(f"\n[{status_color}]{status_text}![/{status_color}]")

        # Create results table
        results_table = Table(title="Incremental Extraction Results")
        results_table.add_column("Metric", style="cyan")
        results_table.add_column("Value", style="magenta")

        results_table.add_row("Years Extracted", str(len(summary["years_extracted"])))
        results_table.add_row("Years Skipped", str(len(summary["years_skipped"])))
        results_table.add_row("Total Rows", f"{summary['total_rows']:,}")
        results_table.add_row("Files Created", str(summary["total_files"]))
        results_table.add_row("Errors", str(len(summary["errors"])))

        console.print(results_table)

        if summary["years_extracted"]:
            console.print(
                f"[green]Extracted years: {', '.join(map(str, summary['years_extracted']))}[/green]"
            )

        if summary["years_skipped"]:
            console.print(
                f"[yellow]Skipped years (current): {', '.join(map(str, summary['years_skipped']))}[/yellow]"
            )

        if summary["errors"]:
            console.print("\n[red]Errors encountered:[/red]")
            for error in summary["errors"]:
                console.print(f"[red]  • {error}[/red]")

    except Exception as e:
        console.print(f"[red]Incremental extraction error: {e}[/red]")
        sys.exit(1)


@extract.command("status")
@click.argument("dataset_name", required=False)
@click.option("--verbose", "-v", is_flag=True, help="Show detailed status information")
def extract_status(dataset_name: str | None, verbose: bool):
    """Show extraction status and history."""
    try:
        manager = ExtractionManager()

        if dataset_name:
            console.print(f"[blue]Extraction status for: {dataset_name}[/blue]")
            summary = manager.get_extraction_summary(dataset_name)

            if not summary["datasets"]:
                console.print(
                    f"[yellow]No extractions recorded for {dataset_name}[/yellow]"
                )
                return

            ds_summary = summary["datasets"][dataset_name]
        else:
            console.print("[blue]Overall extraction status[/blue]")
            summary = manager.get_extraction_summary()

            if summary["total_extractions"] == 0:
                console.print("[yellow]No extractions recorded[/yellow]")
                return

        # Overall summary table
        if not dataset_name:
            overall_table = Table(title="Overall Extraction Summary")
            overall_table.add_column("Metric", style="cyan")
            overall_table.add_column("Value", style="magenta")

            overall_table.add_row("Total Datasets", str(summary["total_datasets"]))
            overall_table.add_row(
                "Total Extractions", str(summary["total_extractions"])
            )
            overall_table.add_row("Successful", str(summary["successful_extractions"]))
            overall_table.add_row("Failed", str(summary["failed_extractions"]))
            if summary["last_extraction"]:
                overall_table.add_row(
                    "Last Extraction", summary["last_extraction"][:19].replace("T", " ")
                )

            console.print(overall_table)

        # Dataset details
        if verbose or dataset_name:
            datasets_to_show = (
                [dataset_name] if dataset_name else summary["datasets"].keys()
            )

            for ds_name in datasets_to_show:
                if ds_name not in summary["datasets"]:
                    continue

                ds_info = summary["datasets"][ds_name]

                ds_table = Table(title=f"Dataset: {ds_name}")
                ds_table.add_column("Year/Type", style="cyan")
                ds_table.add_column("Status", style="magenta")
                ds_table.add_column("Date", style="green")
                ds_table.add_column("Rows", style="blue", justify="right")

                for year_key, year_info in ds_info["years"].items():
                    status_color = (
                        "green" if year_info["status"] == "completed" else "red"
                    )
                    status_text = "✓" if year_info["status"] == "completed" else "✗"

                    ds_table.add_row(
                        year_key,
                        f"[{status_color}]{status_text} {year_info['status']}[/{status_color}]",
                        year_info["date"][:19].replace("T", " "),
                        f"{year_info['rows']:,}" if year_info["rows"] else "0",
                    )

                console.print(ds_table)

    except Exception as e:
        console.print(f"[red]Status check error: {e}[/red]")
        sys.exit(1)


@extract.command("cleanup")
@click.option(
    "--max-age-days",
    default=30,
    type=int,
    help="Maximum age in days before cleanup (default: 30)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be cleaned without actually doing it",
)
@click.option("--verbose", "-v", is_flag=True, help="Show detailed cleanup information")
def extract_cleanup(max_age_days: int, dry_run: bool, verbose: bool):
    """Clean up old extraction files and state entries."""
    try:
        manager = ExtractionManager()

        console.print(f"[blue]Cleanup for files older than {max_age_days} days[/blue]")
        if dry_run:
            console.print("[yellow]DRY RUN - No files will be deleted[/yellow]")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Scanning for old files...", total=None)

            try:
                if not dry_run:
                    cleanup_summary = manager.cleanup_old_extractions(
                        max_age_days=max_age_days
                    )
                else:
                    # For dry run, we'd need to implement a preview function
                    console.print(
                        "[yellow]Dry run functionality would be implemented here[/yellow]"
                    )
                    cleanup_summary = {
                        "files_deleted": 0,
                        "bytes_freed": 0,
                        "state_entries_cleaned": 0,
                        "errors": [],
                    }

                progress.update(task, completed=1, total=1)

            except Exception as e:
                progress.stop()
                if verbose:
                    console.print(f"[red]Detailed error: {type(e).__name__}: {e}[/red]")
                    import traceback

                    console.print(f"[red]Traceback:\n{traceback.format_exc()}[/red]")
                else:
                    console.print(f"[red]Cleanup failed: {e}[/red]")
                    console.print(
                        "[yellow]Use --verbose for detailed error information[/yellow]"
                    )
                sys.exit(1)

        if not dry_run:
            console.print("\n[green]✓ Cleanup completed![/green]")

            # Results table
            cleanup_table = Table(title="Cleanup Results")
            cleanup_table.add_column("Metric", style="cyan")
            cleanup_table.add_column("Value", style="magenta")

            cleanup_table.add_row(
                "Files Deleted", str(cleanup_summary["files_deleted"])
            )
            cleanup_table.add_row("Bytes Freed", f"{cleanup_summary['bytes_freed']:,}")
            cleanup_table.add_row(
                "State Entries Cleaned", str(cleanup_summary["state_entries_cleaned"])
            )
            cleanup_table.add_row("Errors", str(len(cleanup_summary["errors"])))

            console.print(cleanup_table)

            if cleanup_summary["errors"] and verbose:
                console.print("\n[red]Cleanup errors:[/red]")
                for error in cleanup_summary["errors"]:
                    console.print(f"[red]  • {error}[/red]")

    except Exception as e:
        console.print(f"[red]Cleanup error: {e}[/red]")
        sys.exit(1)


@main.command("read")
@click.argument("file_path", type=click.Path(exists=True, path_type=Path))
@click.option("--limit", default=10, help="Number of rows to display (default: 10)")
@click.option("--info", is_flag=True, help="Show file information")
def read_parquet(file_path: Path, limit: int, info: bool):
    """Read and display parquet files."""
    try:
        reader = ParquetReader()

        console.print(f"[blue]Reading parquet file: {file_path}[/blue]")

        if info:
            file_info = reader.get_file_info(file_path)

            info_table = Table(title="Parquet File Information")
            info_table.add_column("Property", style="cyan")
            info_table.add_column("Value", style="magenta")

            for key, value in file_info.items():
                info_table.add_row(key, str(value))

            console.print(info_table)

        data = reader.read_parquet(file_path, limit=limit)

        if data.empty:
            console.print("[yellow]File is empty or contains no data[/yellow]")
            return

        console.print(f"\n[green]Data preview ({len(data)} rows):[/green]")
        console.print(data.to_string())

        console.print(f"\n[cyan]Data shape: {data.shape}[/cyan]")
        console.print(f"[cyan]Columns: {list(data.columns)}[/cyan]")
        console.print(f"[cyan]Data types:\n{data.dtypes.to_string()}[/cyan]")

    except Exception as e:
        console.print(f"[red]Error reading parquet file: {e}[/red]")
        sys.exit(1)


@main.group()
def analytics():
    """Advanced analytics and machine learning operations."""
    pass


@analytics.command("train")
@click.option("--position", default="all", help="Position to train model for (QB, RB, WR, TE, or all)")
@click.option("--season", default=2023, help="Season to use for training")
def train_models(position, season):
    """Train fantasy prediction models."""
    try:
        from src.advanced_analytics import NFLAnalytics
        
        console.print("[cyan]Initializing advanced analytics engine...[/cyan]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Training models...", total=None)
            
            analytics_engine = NFLAnalytics()
            
            # Load and prepare data
            progress.update(task, description="Loading player data...")
            player_data = analytics_engine.load_player_data(season)
            
            if player_data.empty:
                console.print("[red]No data available for training[/red]")
                return
            
            progress.update(task, description="Creating features...")
            player_data = analytics_engine.create_fantasy_features(player_data)
            
            # Train models
            positions = [position] if position != "all" else ["QB", "RB", "WR", "TE"]
            
            results_table = Table(title=f"Model Training Results - Season {season}")
            results_table.add_column("Position", style="cyan")
            results_table.add_column("Best Model", style="green")
            results_table.add_column("R² Score", style="magenta", justify="right")
            results_table.add_column("Records", style="yellow", justify="right")
            
            for pos in positions:
                progress.update(task, description=f"Training {pos} model...")
                model_info = analytics_engine.train_fantasy_model(player_data, pos)
                
                if model_info:
                    results_table.add_row(
                        pos,
                        model_info["best_model"],
                        f"{model_info['best_score']:.3f}",
                        str(len(player_data[player_data['position'] == pos] if pos != "all" else player_data))
                    )
                else:
                    results_table.add_row(pos, "Failed", "N/A", "0")
            
            # Save models
            progress.update(task, description="Saving models...")
            analytics_engine.save_models()
            
            analytics_engine.close()
        
        console.print(results_table)
        console.print("[green]✅ Model training completed successfully![/green]")
        
    except ImportError:
        console.print("[red]❌ Machine learning dependencies not installed. Run: uv add scikit-learn numpy joblib[/red]")
    except Exception as e:
        console.print(f"[red]❌ Training failed: {e}[/red]")
        sys.exit(1)


@analytics.command("predict")
@click.option("--position", default="all", help="Position to predict for")
@click.option("--season", default=2023, help="Season to predict for")
@click.option("--week", help="Specific week to predict (optional)")
@click.option("--player", help="Specific player to predict for (optional)")
def predict_fantasy(position, season, week, player):
    """Generate fantasy predictions."""
    try:
        from src.advanced_analytics import NFLAnalytics
        
        console.print("[cyan]Generating fantasy predictions...[/cyan]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Loading models...", total=None)
            
            analytics_engine = NFLAnalytics()
            
            # Load models
            progress.update(task, description="Loading trained models...")
            analytics_engine.load_models()
            
            if not analytics_engine.models:
                console.print("[yellow]No trained models found. Run 'analytics train' first.[/yellow]")
                return
            
            # Load and prepare data
            progress.update(task, description="Loading player data...")
            player_data = analytics_engine.load_player_data(season)
            
            if player_data.empty:
                console.print("[red]No data available for predictions[/red]")
                return
            
            # Filter data
            if week:
                player_data = player_data[player_data['week'] == int(week)]
            if player:
                player_data = player_data[player_data['player_name'].str.contains(player, case=False)]
            if position != "all":
                player_data = player_data[player_data['position'] == position]
            
            if player_data.empty:
                console.print("[yellow]No data matches the specified filters[/yellow]")
                return
            
            progress.update(task, description="Creating features...")
            player_data = analytics_engine.create_fantasy_features(player_data)
            
            progress.update(task, description="Generating predictions...")
            player_data = analytics_engine.predict_fantasy_points(player_data, position)
            
            analytics_engine.close()
        
        # Display predictions
        predictions_table = Table(title="Fantasy Predictions")
        predictions_table.add_column("Player", style="cyan")
        predictions_table.add_column("Position", style="green")
        predictions_table.add_column("Team", style="yellow")
        predictions_table.add_column("Week", style="white")
        predictions_table.add_column("Predicted", style="magenta", justify="right")
        predictions_table.add_column("Actual", style="blue", justify="right")
        predictions_table.add_column("Confidence", style="red", justify="right")
        
        # Sort by predicted points
        top_predictions = player_data.nlargest(20, 'predicted_fantasy_points')
        
        for _, row in top_predictions.iterrows():
            predictions_table.add_row(
                row['player_name'],
                row['position'],
                row['team'],
                str(row['week']),
                f"{row['predicted_fantasy_points']:.1f}",
                f"{row['fantasy_points_ppr']:.1f}" if pd.notna(row['fantasy_points_ppr']) else "N/A",
                f"{row['prediction_confidence']:.2f}"
            )
        
        console.print(predictions_table)
        console.print(f"[green]✅ Generated predictions for {len(player_data)} player-week combinations[/green]")
        
    except ImportError:
        console.print("[red]❌ Machine learning dependencies not installed. Run: uv add scikit-learn numpy joblib[/red]")
    except Exception as e:
        console.print(f"[red]❌ Prediction failed: {e}[/red]")
        sys.exit(1)


@analytics.command("insights")
@click.option("--season", default=2023, help="Season to analyze")
def generate_insights(season):
    """Generate comprehensive analytics insights."""
    try:
        from src.advanced_analytics import NFLAnalytics
        
        console.print("[cyan]Generating analytics insights...[/cyan]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Analyzing data...", total=None)
            
            analytics_engine = NFLAnalytics()
            
            progress.update(task, description="Generating insights report...")
            insights = analytics_engine.generate_insights_report()
            
            analytics_engine.close()
        
        if not insights:
            console.print("[yellow]Unable to generate insights - insufficient data[/yellow]")
            return
        
        # Display insights
        console.print(f"\n[bold cyan]📊 NFL Analytics Insights - Season {season}[/bold cyan]")
        
        # Summary stats
        if 'summary_stats' in insights:
            stats = insights['summary_stats']
            summary_table = Table(title="Summary Statistics")
            summary_table.add_column("Metric", style="cyan")
            summary_table.add_column("Value", style="green", justify="right")
            
            summary_table.add_row("Total Players", str(stats.get('total_players', 0)))
            summary_table.add_row("Total Teams", str(stats.get('total_teams', 0)))
            summary_table.add_row("Avg Fantasy Points", f"{stats.get('avg_fantasy_points', 0):.1f}")
            summary_table.add_row("Avg Team EPA", f"{stats.get('avg_team_epa', 0):.3f}")
            
            console.print(summary_table)
        
        # Top consistent players
        if 'top_consistent_players' in insights:
            console.print("\n[bold yellow]🏆 Most Consistent Fantasy Players[/bold yellow]")
            consistent_table = Table()
            consistent_table.add_column("Player", style="cyan")
            consistent_table.add_column("Position", style="green")
            consistent_table.add_column("Team", style="yellow")
            consistent_table.add_column("Avg Points", style="magenta", justify="right")
            consistent_table.add_column("Consistency", style="red", justify="right")
            
            for player in insights['top_consistent_players'][:10]:
                consistent_table.add_row(
                    player['player_name'],
                    player['position'],
                    player['team'],
                    f"{player['avg_points']:.1f}",
                    f"{player['consistency_score']:.2f}"
                )
            
            console.print(consistent_table)
        
        # Strongest teams
        if 'strongest_teams' in insights:
            console.print("\n[bold green]💪 Strongest Teams by EPA[/bold green]")
            teams_table = Table()
            teams_table.add_column("Team", style="cyan")
            teams_table.add_column("Avg EPA", style="green", justify="right")
            teams_table.add_column("Win Rate", style="yellow", justify="right")
            teams_table.add_column("Pass EPA", style="magenta", justify="right")
            teams_table.add_column("Rush EPA", style="red", justify="right")
            
            for team in insights['strongest_teams'][:10]:
                teams_table.add_row(
                    team['team'],
                    f"{team['avg_epa']:.3f}",
                    f"{team['win_rate']:.1%}",
                    f"{team['avg_pass_epa']:.3f}",
                    f"{team['avg_rush_epa']:.3f}"
                )
            
            console.print(teams_table)
        
        console.print("[green]✅ Insights generated successfully![/green]")
        
    except ImportError:
        console.print("[red]❌ Machine learning dependencies not installed. Run: uv add scikit-learn numpy joblib[/red]")
    except Exception as e:
        console.print(f"[red]❌ Insights generation failed: {e}[/red]")
        sys.exit(1)


@main.group()
def realtime():
    """Real-time data processing and streaming operations."""
    pass


@realtime.command("start")
@click.option("--duration", default=0, help="Duration to run (0 for continuous)")
@click.option("--port", default=8765, help="WebSocket port")
def start_processor(duration, port):
    """Start the real-time NFL data processor."""
    try:
        from src.realtime_processor import NFLRealtimeProcessor, FantasyLiveTracker
        
        console.print("[cyan]🚀 Starting NFL Real-time Processor...[/cyan]")
        
        # Initialize processor
        processor = NFLRealtimeProcessor()
        
        # Initialize fantasy tracker
        fantasy_tracker = FantasyLiveTracker(processor)
        
        # Event subscribers for CLI display
        def score_alert(event):
            data = event.data
            console.print(f"[green]🏈 SCORE: {event.home_team} {data.get('new_home_score', 0)} - {event.away_team} {data.get('new_away_score', 0)}[/green]")
        
        def fantasy_alert(event):
            players_affected = len(event.data.get('fantasy_players_affected', []))
            console.print(f"[yellow]⭐ FANTASY: {players_affected} players affected in {event.game_id}[/yellow]")
        
        processor.subscribe('score_update', score_alert)
        processor.subscribe('fantasy_update', fantasy_alert)
        
        # Start processor
        processor.start()
        
        console.print(f"[green]✅ Real-time processor started on port {port}[/green]")
        console.print("[cyan]📡 WebSocket server: ws://localhost:8765[/cyan]")
        console.print("[cyan]💻 Connect with: websocat ws://localhost:8765[/cyan]")
        console.print("[yellow]Press Ctrl+C to stop[/yellow]")
        
        # Run for specified duration or continuously
        try:
            if duration > 0:
                console.print(f"[cyan]Running for {duration} seconds...[/cyan]")
                import time
                time.sleep(duration)
            else:
                console.print("[cyan]Running continuously... Press Ctrl+C to stop[/cyan]")
                while True:
                    import time
                    time.sleep(1)
                    
        except KeyboardInterrupt:
            console.print("\n[yellow]Stopping processor...[/yellow]")
        
        processor.stop()
        console.print("[green]✅ Real-time processor stopped[/green]")
        
    except ImportError:
        console.print("[red]❌ Real-time dependencies not installed. Run: uv add websockets aiohttp requests[/red]")
    except Exception as e:
        console.print(f"[red]❌ Failed to start processor: {e}[/red]")
        sys.exit(1)


@realtime.command("stream")
@click.option("--duration", default=60, help="Duration to run stream processing")
@click.option("--workers", default=4, help="Number of worker threads")
def start_stream_processor(duration, workers):
    """Start the high-performance stream processor."""
    try:
        import asyncio
        from src.stream_processor import StreamProcessor, NFLStreamAnalyzer, simulate_live_events
        
        console.print("[cyan]🌊 Starting Stream Processor...[/cyan]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Initializing stream processor...", total=None)
            
            # Initialize processor
            processor = StreamProcessor(max_workers=workers)
            analyzer = NFLStreamAnalyzer(processor)
            
            progress.update(task, description="Creating analysis windows...")
            analyzer.create_analysis_windows()
            
            progress.update(task, description="Starting processor...")
            processor.start()
            
            progress.update(task, description=f"Running simulation for {duration} seconds...")
            asyncio.run(simulate_live_events(processor, duration))
            
            # Show final stats
            stats = processor.get_stats()
            processor.stop()
        
        # Display results
        results_table = Table(title=f"Stream Processing Results ({duration}s)")
        results_table.add_column("Metric", style="cyan")
        results_table.add_column("Value", style="green", justify="right")
        
        results_table.add_row("Events Processed", str(stats['events_processed']))
        results_table.add_row("Windows Processed", str(stats['windows_processed']))
        results_table.add_row("Events/Second", f"{stats['events_per_second']:.2f}")
        results_table.add_row("Active Windows", str(stats['active_windows']))
        results_table.add_row("Errors", str(stats['errors']))
        results_table.add_row("Runtime", f"{stats['runtime_seconds']:.1f}s")
        
        console.print(results_table)
        console.print("[green]✅ Stream processing completed successfully![/green]")
        
    except ImportError:
        console.print("[red]❌ Stream processing dependencies not installed. Run: uv add websockets aiohttp requests[/red]")
    except Exception as e:
        console.print(f"[red]❌ Stream processing failed: {e}[/red]")
        sys.exit(1)


@realtime.command("dashboard")
def launch_realtime_dashboard():
    """Launch the real-time Streamlit dashboard."""
    try:
        import subprocess
        import os
        
        console.print("[cyan]🚀 Launching real-time dashboard...[/cyan]")
        
        # Path to the real-time dashboard
        dashboard_path = "visualizations/streamlit_app/realtime_dashboard.py"
        
        if not os.path.exists(dashboard_path):
            console.print("[red]❌ Real-time dashboard not found[/red]")
            sys.exit(1)
        
        console.print("[green]🌐 Starting Streamlit server...[/green]")
        console.print("[cyan]📊 Dashboard will open at: http://localhost:8501[/cyan]")
        console.print("[yellow]Press Ctrl+C to stop[/yellow]")
        
        # Launch Streamlit
        subprocess.run(["streamlit", "run", dashboard_path])
        
    except FileNotFoundError:
        console.print("[red]❌ Streamlit not installed. Run: uv add streamlit[/red]")
    except Exception as e:
        console.print(f"[red]❌ Failed to launch dashboard: {e}[/red]")
        sys.exit(1)


@realtime.command("status")
def show_realtime_status():
    """Show status of real-time processing components."""
    try:
        import websockets
        import asyncio
        import requests
        from datetime import datetime
        
        console.print("[cyan]📊 Real-time System Status[/cyan]")
        
        # Check WebSocket server
        async def check_websocket():
            try:
                async with websockets.connect("ws://localhost:8765") as websocket:
                    return "✅ Connected"
            except Exception:
                return "❌ Not available"
        
        ws_status = asyncio.run(check_websocket())
        
        # Create status table
        status_table = Table(title="Component Status")
        status_table.add_column("Component", style="cyan")
        status_table.add_column("Status", style="green")
        status_table.add_column("Details", style="yellow")
        
        status_table.add_row("WebSocket Server", ws_status, "ws://localhost:8765")
        status_table.add_row("Stream Processor", "⚠️ Manual start", "Use 'realtime stream' command")
        status_table.add_row("Real-time Dashboard", "⚠️ Manual start", "Use 'realtime dashboard' command")
        status_table.add_row("Database", "✅ Available", "DuckDB backend")
        
        console.print(status_table)
        
        # Show recent activity if available
        try:
            from src.realtime_processor import NFLRealtimeProcessor
            processor = NFLRealtimeProcessor()
            # Try to get some basic info without starting
            console.print("\n[cyan]📈 System Information[/cyan]")
            console.print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            console.print("Use 'realtime start' to begin processing live events")
            
        except Exception as e:
            console.print(f"\n[yellow]⚠️ Unable to get detailed status: {e}[/yellow]")
        
        console.print("\n[green]✅ Status check completed[/green]")
        
    except ImportError:
        console.print("[red]❌ Real-time dependencies not available[/red]")
    except Exception as e:
        console.print(f"[red]❌ Status check failed: {e}[/red]")


@main.group()
def api():
    """API server management and operations."""
    pass


@api.command("start")
@click.option("--host", default="0.0.0.0", help="Host to bind to")
@click.option("--port", default=8000, help="Port to bind to")
@click.option("--reload", default=True, help="Enable auto-reload for development")
def start_api_server(host, port, reload):
    """Start the FastAPI server with OpenAPI documentation."""
    try:
        console.print("[cyan]🚀 Starting NFL Analytics Platform API Server...[/cyan]")
        console.print(f"[green]📊 API Documentation: http://{host}:{port}/docs[/green]")
        console.print(f"[green]📖 ReDoc Documentation: http://{host}:{port}/redoc[/green]")
        console.print(f"[green]🔗 OpenAPI JSON: http://{host}:{port}/api/v1/openapi.json[/green]")
        console.print("[yellow]Press Ctrl+C to stop[/yellow]")
        
        # Import and start the API server
        from src.api.main import app
        import uvicorn
        
        uvicorn.run(
            "src.api.main:app",
            host=host,
            port=port,
            reload=reload,
            log_level="info"
        )
        
    except ImportError:
        console.print("[red]❌ FastAPI dependencies not installed. Run: uv add fastapi uvicorn pydantic[/red]")
    except Exception as e:
        console.print(f"[red]❌ API server failed to start: {e}[/red]")
        sys.exit(1)


@api.command("docs")
def generate_api_docs():
    """Generate and validate OpenAPI documentation."""
    try:
        console.print("[cyan]📖 Generating OpenAPI documentation...[/cyan]")
        
        from src.api.main import app
        import json
        from pathlib import Path
        
        # Get OpenAPI schema
        openapi_schema = app.openapi()
        
        # Save to file
        docs_path = Path("docs/openapi_generated.json")
        docs_path.parent.mkdir(exist_ok=True)
        
        with open(docs_path, "w") as f:
            json.dump(openapi_schema, f, indent=2)
        
        console.print(f"[green]✅ OpenAPI documentation generated: {docs_path}[/green]")
        console.print(f"[green]📊 Total endpoints: {len(openapi_schema.get('paths', {}))}/[green]")
        console.print(f"[green]📋 Components defined: {len(openapi_schema.get('components', {}).get('schemas', {}))}/[green]")
        
    except ImportError:
        console.print("[red]❌ FastAPI not available for documentation generation[/red]")
    except Exception as e:
        console.print(f"[red]❌ Documentation generation failed: {e}[/red]")


@api.command("validate")
@click.option("--spec-file", default="openapi.yaml", help="OpenAPI specification file to validate")
def validate_openapi_spec(spec_file):
    """Validate OpenAPI specification file."""
    try:
        from pathlib import Path
        import yaml
        
        spec_path = Path(spec_file)
        if not spec_path.exists():
            console.print(f"[red]❌ Specification file not found: {spec_path}[/red]")
            sys.exit(1)
        
        console.print(f"[cyan]🔍 Validating OpenAPI specification: {spec_path}[/cyan]")
        
        # Load and validate YAML
        with open(spec_path, "r") as f:
            spec = yaml.safe_load(f)
        
        # Basic validation checks
        required_fields = ["openapi", "info", "paths"]
        missing_fields = [field for field in required_fields if field not in spec]
        
        if missing_fields:
            console.print(f"[red]❌ Missing required fields: {', '.join(missing_fields)}[/red]")
            sys.exit(1)
        
        # Count components
        paths_count = len(spec.get("paths", {}))
        components_count = len(spec.get("components", {}).get("schemas", {}))
        
        console.print(f"[green]✅ OpenAPI specification is valid[/green]")
        console.print(f"[cyan]📊 API Version: {spec['info']['version']}[/cyan]")
        console.print(f"[cyan]🔗 Total Endpoints: {paths_count}[/cyan]")
        console.print(f"[cyan]📋 Schema Components: {components_count}[/cyan]")
        
    except yaml.YAMLError as e:
        console.print(f"[red]❌ YAML parsing error: {e}[/red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]❌ Validation failed: {e}[/red]")
        sys.exit(1)


@main.group()
def models():
    """Work with dbt staging models through DuckLake."""
    pass


@models.command("list")
def list_models():
    """List all available dbt staging models."""
    try:
        ducklake = DuckLakeManager()
        models = ducklake.list_available_models()
        
        table = Table(title="Available dbt Staging Models")
        table.add_column("Model", style="cyan", no_wrap=True)
        table.add_column("Description", style="magenta")
        table.add_column("Schema", style="green", justify="center")
        table.add_column("Base Table", style="yellow")
        
        for model in models:
            table.add_row(
                model["name"],
                model["description"],
                model["schema"],
                model["table"]
            )
        
        console.print(table)
        console.print(f"\n[blue]💡 Use 'models query <model_name>' to query a model[/blue]")
        console.print(f"[blue]💡 Use 'models materialize' to refresh all staging models[/blue]")
        
    except Exception as e:
        console.print(f"[red]Error listing models: {e}[/red]")
        sys.exit(1)


@models.command("materialize")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed materialization output")
def materialize_models(verbose: bool):
    """Materialize all dbt staging models via Dagster."""
    try:
        ducklake = DuckLakeManager()
        
        console.print("[blue]🔄 Triggering dbt staging model materialization via Dagster...[/blue]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Materializing staging models...", total=None)
            
            result = ducklake.materialize_staging_models()
            progress.update(task, completed=1, total=1)
        
        if result["success"]:
            console.print("[green]✅ Materialization completed successfully![/green]")
            if verbose and result["stdout"]:
                console.print(f"\n[cyan]Output:[/cyan]\n{result['stdout']}")
        else:
            console.print("[red]❌ Materialization failed![/red]")
            if result["stderr"]:
                console.print(f"[red]Error: {result['stderr']}[/red]")
            sys.exit(1)
            
    except Exception as e:
        console.print(f"[red]Error triggering materialization: {e}[/red]")
        sys.exit(1)


@models.command("query")
@click.argument("model_name")
@click.option("--limit", default=10, help="Number of rows to display (default: 10)")
@click.option("--as-of-date", help="Query as of specific date (YYYY-MM-DD) for time travel")
@click.option("--show-schema", is_flag=True, help="Show model schema information")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed error information")
def query_model(model_name: str, limit: int, as_of_date: str | None, show_schema: bool, verbose: bool):
    """Query a dbt staging model through DuckLake."""
    try:
        ducklake = DuckLakeManager()
        
        # Validate model exists
        try:
            model_info = ducklake.get_model_info(model_name)
        except KeyError:
            available_models = [m["name"] for m in ducklake.list_available_models()]
            console.print(f"[red]Invalid model: {model_name}[/red]")
            console.print(f"[yellow]Available models: {', '.join(available_models)}[/yellow]")
            sys.exit(1)
        
        if show_schema:
            console.print(f"[blue]📋 Schema for {model_name}[/blue]")
            schema_info = ducklake.get_model_schema(model_name)
            
            schema_table = Table(title=f"Schema: {model_name}")
            schema_table.add_column("Column", style="cyan")
            schema_table.add_column("Type", style="magenta")
            
            for col in schema_info["columns"]:
                schema_table.add_row(col["column_name"], col["column_type"])
            
            console.print(schema_table)
            console.print()
        
        # Build query description
        query_desc = f"Querying {model_name}"
        if as_of_date:
            query_desc += f" as of {as_of_date}"
        if limit:
            query_desc += f" (limit {limit} rows)"
        
        console.print(f"[blue]🔍 {query_desc}[/blue]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Executing query...", total=None)
            
            data = ducklake.query_model(model_name, limit=limit, as_of_date=as_of_date)
            progress.update(task, completed=1, total=1)
        
        if data.empty:
            console.print(f"[yellow]No data found for {model_name}[/yellow]")
            return
        
        # Show time travel info if applicable
        if as_of_date and hasattr(data, 'attrs') and 'ducklake_version_date' in data.attrs:
            console.print(f"[cyan]📅 Data version used: {data.attrs['ducklake_version_date']}[/cyan]")
        
        console.print(f"\n[green]Results ({len(data)} rows):[/green]")
        console.print(data.to_string())
        
        console.print(f"\n[cyan]Data shape: {data.shape}[/cyan]")
        console.print(f"[cyan]Columns: {list(data.columns)}[/cyan]")
        
    except Exception as e:
        if verbose:
            console.print(f"[red]Detailed error: {type(e).__name__}: {e}[/red]")
            import traceback
            console.print(f"[red]Traceback:\n{traceback.format_exc()}[/red]")
        else:
            console.print(f"[red]Error querying model: {e}[/red]")
            console.print("[yellow]Use --verbose for detailed error information[/yellow]")
        sys.exit(1)


@models.command("catalog")
def show_catalog():
    """Show all tables registered in DuckLake catalog."""
    try:
        ducklake = DuckLakeManager()
        tables = ducklake.get_catalog_tables()
        
        if not tables:
            console.print("[yellow]No tables found in DuckLake catalog[/yellow]")
            console.print("[blue]💡 Run 'models materialize' to create staging models[/blue]")
            return
        
        table = Table(title="DuckLake Catalog Tables")
        table.add_column("Schema", style="cyan")
        table.add_column("Table", style="magenta")
        table.add_column("Versions", style="green", justify="center")
        table.add_column("Latest ETL", style="yellow")
        table.add_column("Total Rows", style="blue", justify="right")
        table.add_column("Created", style="dim")
        
        for t in tables:
            table.add_row(
                t["schema_name"],
                t["table_name"],
                str(t["version_count"]),
                str(t["latest_etl_date"]) if t["latest_etl_date"] else "N/A",
                f"{t['total_rows']:,}" if t['total_rows'] else "0",
                t["created_at"].strftime("%Y-%m-%d") if t["created_at"] else "N/A"
            )
        
        console.print(table)
        console.print(f"\n[blue]💡 Use 'models versions <schema>.<table>' to see version history[/blue]")
        
    except Exception as e:
        console.print(f"[red]Error accessing catalog: {e}[/red]")
        sys.exit(1)


@models.command("versions")
@click.argument("table_name")  # Format: schema.table
def show_versions(table_name: str):
    """Show version history for a specific table."""
    try:
        if "." not in table_name:
            console.print("[red]Table name must be in format 'schema.table'[/red]")
            console.print("[yellow]Example: nfl_raw.pbp[/yellow]")
            sys.exit(1)
        
        schema_name, table = table_name.split(".", 1)
        
        ducklake = DuckLakeManager()
        versions = ducklake.get_table_versions(schema_name, table)
        
        if not versions:
            console.print(f"[yellow]No versions found for {schema_name}.{table}[/yellow]")
            return
        
        table_widget = Table(title=f"Version History: {schema_name}.{table}")
        table_widget.add_column("Version", style="cyan", justify="center")
        table_widget.add_column("ETL Date", style="magenta")
        table_widget.add_column("Rows", style="green", justify="right")
        table_widget.add_column("File Size", style="yellow", justify="right")
        table_widget.add_column("Created", style="blue")
        table_widget.add_column("File Path", style="dim")
        
        for v in versions:
            file_size = f"{v['file_size'] / 1024 / 1024:.1f} MB" if v['file_size'] else "N/A"
            table_widget.add_row(
                str(v["version_number"]),
                str(v["etl_date"]),
                f"{v['row_count']:,}" if v['row_count'] else "0",
                file_size,
                v["created_at"].strftime("%Y-%m-%d %H:%M") if v["created_at"] else "N/A",
                str(v["file_path"])[-50:] + "..." if len(str(v["file_path"])) > 50 else str(v["file_path"])
            )
        
        console.print(table_widget)
        console.print(f"\n[blue]💡 Use 'models query <model> --as-of-date YYYY-MM-DD' for time travel[/blue]")
        
    except Exception as e:
        console.print(f"[red]Error getting version history: {e}[/red]")
        sys.exit(1)


@models.command("sql")
@click.argument("query", required=False)
@click.option("--file", "-f", help="Read SQL query from file")
def run_sql(query: str | None, file: str | None):
    """Run custom SQL query against DuckLake data."""
    try:
        if not query and not file:
            console.print("[red]Either provide a query or use --file to read from file[/red]")
            console.print("[yellow]Example: models sql 'SELECT * FROM \\'data/team_desc/etl_date=*/data.parquet\\' LIMIT 5'[/yellow]")
            sys.exit(1)
        
        if file:
            if not Path(file).exists():
                console.print(f"[red]SQL file not found: {file}[/red]")
                sys.exit(1)
            with open(file, "r") as f:
                query = f.read()
        
        ducklake = DuckLakeManager()
        
        console.print(f"[blue]🔍 Executing custom SQL query...[/blue]")
        console.print(f"[dim]Query: {query[:100]}{'...' if len(query) > 100 else ''}[/dim]")
        
        with Progress(
            SpinnerColumn(), 
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Executing query...", total=None)
            
            result = ducklake.run_custom_query(query)
            progress.update(task, completed=1, total=1)
        
        if result.empty:
            console.print("[yellow]Query returned no results[/yellow]")
            return
        
        console.print(f"\n[green]Results ({len(result)} rows):[/green]")
        console.print(result.to_string())
        
        console.print(f"\n[cyan]Data shape: {result.shape}[/cyan]")
        console.print(f"[cyan]Columns: {list(result.columns)}[/cyan]")
        
    except Exception as e:
        console.print(f"[red]Error executing SQL query: {e}[/red]")
        sys.exit(1)


if __name__ == "__main__":
    main()
