import nfl_data_py as nfl

DATA_TABLE_MAP = {
    "pbp_data": {
        "fetch_func": nfl.import_pbp_data,
        "table_name": "pbp",
        "downcast_type": False,
        "year_accepted": True,
    },
    "weekly_data": {
        "fetch_func": nfl.import_weekly_data,
        "table_name": "weekly",
        "downcast_type": False,
        "year_accepted": True,
    },
}
