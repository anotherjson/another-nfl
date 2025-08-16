from dataclasses import dataclass


@dataclass
class NFLVerseSource:
    """A class to setup the config for NFLVerse datasets in github."""

    source_name: str
    source_prefix: str
    file_type: str
    start_year: int
    local_path: str


@dataclass
class NFLVerseGlobal:
    """A class to setup common NFLVerse settings."""

    name: str
    url_prefix: str
    database_path: str


NFLVERSE_GLOBAL_CONFIG = NFLVerseGlobal(
    name="core",
    url_prefix="https://github.com/nflverse/nflverse-data/releases/download/",
    database_path="data/nflverse.db",
)


NFLVERSE_SOURCES = [
    NFLVerseSource(
        source_name="weekly",
        source_prefix="player_stats/player_stats_",
        file_type=".parquet",
        start_year=1999,
        local_path="data/raw/weekly",
    ),
]
