"""NFL Data CLI Tool for exploration and debugging."""
import sys
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.table import Table

from src.nfl_explorer import NFLExplorer
from src.parquet_reader import ParquetReader

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
                str(dataset["start_year"]) if dataset["start_year"] else "N/A"
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
def explore_data(dataset: str, year: Optional[int], limit: int, verbose: bool):
    """Show sample data from a specific NFL dataset."""
    try:
        explorer = NFLExplorer()
        
        if not explorer.is_valid_dataset(dataset):
            available = [d["name"] for d in explorer.list_datasets()]
            console.print(f"[red]Invalid dataset: {dataset}[/red]")
            console.print(f"[yellow]Available datasets: {', '.join(available)}[/yellow]")
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
            console.print("[yellow]Use --verbose for detailed error information[/yellow]")
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


if __name__ == "__main__":
    main()