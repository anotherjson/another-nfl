# Project NFL DATA PY Extraction

## Summary

the goal is to extract data using the nfl_data_py python package and have that data available in a datalake.

## Tools and uses

- create readme and any additional documentation
- ansible should be use to build the project fresh and setting up enviroment variables
  -- no tool should be installed globally besides ansible, uv, git, and github cli
  -- check if ansible and uv are installed globally first before attempting to install them
  -- build should work with dev and prod enviroments
  -- enviroment variables, db information, and other sensitive information should ignored by git
- uv should be used to manage the virtual enviroments and python packages
  -- ruff should be used for code formatting and linting
- nfl_data_py python package is the data source and has functions for pulling data from multiple datasets
  -- each dataset has different starting years for data
  -- folders should exist for each dataset
  -- data from each dataset should be extracted by year and partitioned by etl_date
- parquet is the file format files should be saved in
- an intital cli tool should be built using click
  -- to extract data using nfl_data_py for exploration and debugging and printing to console
  -- to read parquet files for exploration and debugging and printing to console
  -- all cli tool functions should have tests
- For containerization, podman should be used
- For service orchestration, docker compose should be used
- dagster should orchestrate all code around extracting the datasets
  -- project structure should match dagster best practices
  -- all dagster assets should have tests
- dbt should be used for data transformations
  -- dagster should orchestrate dbt
  -- the initial model should be staging and do minor cleaning of the raw data
  -- an intermidate model should split the data into models for different teams and position players
  -- a final model layer that are used by dashboard tools and ml models
- ducklake should be used for data lakes
  -- metadata is stored in postgres
  -- data is stored in parquet

## General paradigm

use a functional programing paradigm

## Plan phases

### First phase

1. Create a detail plan for doing the first phase
2. Setup project enviroment
3. Setup uv
4. Create project structure
5. build cli tool to explore nfl_data_py
6. build tests for the cli tools
7. test the cli tools
8. show phase is working with console outputs
9. get approval for next phase

### Second phase

6. Create a detail plan for doing the second phase
7. based on the nfl_data_py documentation and cli tool, create a config for each nfl_data_py dataset
8. build functions for extracting data from nfl_data_py
9. build tests for the extracting functions
10. test the extracting functions
11. get approval for next phase

### Third phase

12. Create a detail plan for doing the third phase
13. build out dbt structure, use both dbt and dagster best practices
14. create dbt staging models for light transformations of the raw datasets
15. create dbt docs md files for the schemas
16. update dagster files to work with dbt models
17. build tests for dbt models and functions
18. test the dbt models and functions
19. get approval for next phase

## Additional phases pending

20. create a documentation for a suggested plan for the next phases
