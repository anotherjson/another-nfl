import click
import nfl_data_py as nfl
import pandas as pd

@click.group()
def cli():
    """A CLI to explore nfl_data_py."""
    pass

@cli.command()
def list_datasets():
    """Prints a list of available dataset types."""
    datasets = [
        "play-by-play",
        "weekly",
        "seasonal",
        "rosters",
        "schedules",
        "draft_picks",
        "combine",
        "player_info",
    ]
    click.echo("Available dataset types:")
    for dataset in datasets:
        click.echo(f"- {dataset}")

@cli.command()
@click.argument('year', type=int)
def get_pbp(year):
    """Fetches and prints a sample of play-by-play data for a given year."""
    df = nfl.import_pbp_data([year])
    click.echo(df.head().to_string())

@cli.command()
@click.argument('file_path', type=click.Path(exists=True))
def read_parquet(file_path):
    """Reads and prints the content of a Parquet file."""
    df = pd.read_parquet(file_path)
    click.echo(df.to_string())

if __name__ == '__main__':
    cli()
