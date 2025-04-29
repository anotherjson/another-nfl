"""[TODO:description]"""

# Python packages
import functools
from collections.abc import Callable

# 3rd party packages
import nfl_data_py as nfl


def build_nfl_function(config: dict) -> Callable:
    """[TODO:description]"""
    nfl_func = config["fetch_func"]
    if isinstance(config["downcast_type"], bool):
        nfl_func = functools.partial(nfl_func, downcast=config["downcast_type"])

    if isinstance(config["s_type"], str):
        nfl_func = functools.partial(nfl_func, s_type=config["s_type"])

    if isinstance(config["positions"], str):
        nfl_func = functools.partial(nfl_func, positions=config["positions"])

    if isinstance(config["stat_type"], str):
        nfl_func = functools.partial(nfl_func, stat_type=config["stat_type"])

    if isinstance(config["level"], str):
        nfl_func = functools.partial(nfl_func, level=config["level"])

    if isinstance(config["frequency"], str):
        nfl_func = functools.partial(nfl_func, frequency=config["frequency"])

    return nfl_func


DATA_TABLE_MAP = {
    "pbp_data": {
        "fetch_func": nfl.import_pbp_data,
        "table_name": "pbp",
        "downcast_type": False,
        "year_accepted": True,
        "s_type": None,
        "positions": None,
        "stat_type": None,
        "level": None,
        "frequency": None,
    },
    "weekly_data": {
        "fetch_func": nfl.import_weekly_data,
        "table_name": "weekly",
        "downcast_type": False,
        "year_accepted": True,
        "s_type": None,
        "positions": None,
        "stat_type": None,
        "level": None,
        "frequency": None,
    },
    "seasonal_data": {
        "fetch_func": nfl.import_seasonal_data,
        "table_name": "seasonal",
        "downcast_type": None,
        "year_accepted": True,
        "s_type": ["All"],
        "positions": None,
        "stat_type": None,
        "level": None,
        "frequency": None,
    },
    "seasonal_rosters": {
        "fetch_func": nfl.import_seasonal_rosters,
        "table_name": "seasonal_rosters",
        "downcast_type": None,
        "year_accepted": True,
        "s_type": None,
        "positions": None,
        "stat_type": None,
        "level": None,
        "frequency": None,
    },
    "weekly_rosters": {
        "fetch_func": nfl.import_weekly_rosters,
        "table_name": "weekly_rosters",
        "downcast_type": None,
        "year_accepted": True,
        "s_type": None,
        "positions": None,
        "stat_type": None,
        "level": None,
        "frequency": None,
    },
    "win_totals": {
        "fetch_func": nfl.import_win_totals,
        "table_name": "win_totals",
        "downcast_type": None,
        "year_accepted": False,
        "s_type": None,
        "positions": None,
        "stat_type": None,
        "level": None,
        "frequency": None,
    },
    "sc_lines": {
        "fetch_func": nfl.import_sc_lines,
        "table_name": "sc_lines",
        "downcast_type": None,
        "year_accepted": False,
        "s_type": None,
        "positions": None,
        "stat_type": None,
        "level": None,
        "frequency": None,
    },
    "officials": {
        "fetch_func": nfl.import_officials,
        "table_name": "officials",
        "downcast_type": None,
        "year_accepted": False,
        "s_type": None,
        "positions": None,
        "stat_type": None,
        "level": None,
        "frequency": None,
    },
    "draft_picks": {
        "fetch_func": nfl.import_draft_picks,
        "table_name": "draft_picks",
        "downcast_type": None,
        "year_accepted": False,
        "s_type": None,
        "positions": None,
        "stat_type": None,
        "level": None,
        "frequency": None,
    },
    "draft_values": {
        "fetch_func": nfl.import_draft_values,
        "table_name": "draft_values",
        "downcast_type": None,
        "year_accepted": False,
        "s_type": None,
        "positions": None,
        "stat_type": None,
        "level": None,
        "frequency": None,
    },
    "team_desc": {
        "fetch_func": nfl.import_team_desc,
        "table_name": "teams",
        "downcast_type": None,
        "year_accepted": False,
        "s_type": None,
        "positions": None,
        "stat_type": None,
        "level": None,
        "frequency": None,
    },
    "schedules": {
        "fetch_func": nfl.import_schedules,
        "table_name": "schedules",
        "downcast_type": None,
        "year_accepted": True,
        "s_type": None,
        "positions": None,
        "stat_type": None,
        "level": None,
        "frequency": None,
    },
    "combine_data": {
        "fetch_func": nfl.import_combine_data,
        "table_name": "combine",
        "downcast_type": None,
        "year_accepted": False,
        "s_type": None,
        "positions": None,
        "stat_type": None,
        "level": None,
        "frequency": None,
    },
    "player_ids": {
        "fetch_func": nfl.import_ids,
        "table_name": "player_ids",
        "downcast_type": None,
        "year_accepted": False,
        "s_type": None,
        "positions": None,
        "stat_type": None,
        "level": None,
        "frequency": None,
    },
    "depth_charts": {
        "fetch_func": nfl.import_depth_charts,
        "table_name": "depth_charts",
        "downcast_type": None,
        "year_accepted": True,
        "s_type": None,
        "positions": None,
        "stat_type": None,
        "level": None,
        "frequency": None,
    },
    "injuries": {
        "fetch_func": nfl.import_injuries,
        "table_name": "injuries",
        "downcast_type": None,
        "year_accepted": True,
        "s_type": None,
        "positions": None,
        "stat_type": None,
        "level": None,
        "frequency": None,
    },
    "qbr_nfl": {
        "fetch_func": nfl.import_qbr,
        "table_name": "qbr_nfl",
        "downcast_type": None,
        "year_accepted": False,
        "s_type": None,
        "positions": None,
        "stat_type": None,
        "level": "nfl",
        "frequency": "weekly",
    },
    "qbr_college": {
        "fetch_func": nfl.import_qbr,
        "table_name": "qbr_college",
        "downcast_type": None,
        "year_accepted": False,
        "s_type": None,
        "positions": None,
        "stat_type": None,
        "level": "college",
        "frequency": "weekly",
    },
    "snap_counts": {
        "fetch_func": nfl.import_snap_counts,
        "table_name": "snap_counts",
        "downcast_type": None,
        "year_accepted": True,
        "s_type": None,
        "positions": None,
        "stat_type": None,
        "level": None,
        "frequency": None,
    },
}
