from dataclasses import dataclass
from functools import partial


@dataclass
class NFLVerseGlobal:
    """A class to setup common NFLVerse settings."""

    name: str
    url_prefix: str
    database_path: str


@dataclass
class NFLVerseSource:
    """A class to setup the config for NFLVerse datasets in github."""

    source_name: str
    source_prefix: str
    file_type: str
    source_start_year: int
    local_path: str


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
        source_start_year=1999,
        local_path="data/raw/weekly",
    ),
]

AVAILABLE_SOURCES = [s.source_name for s in NFLVERSE_SOURCES]


def find_source_config(
    source_name: str,
    source_list: list[NFLVerseSource],
) -> NFLVerseSource:
    for source in source_list:
        if source_name == source.source_name:
            return source

    raise ValueError(f"Source is not found in source_list")


make_config_available = partial(find_source_config, source_list=NFLVERSE_SOURCES)
