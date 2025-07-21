# Project Plan: NFL Data Extraction and Processing

This document outlines the plan for building a data pipeline to extract NFL data, store it in a data lake, and transform it for analysis.

## Project Goal

The primary objective is to extract data using the `nfl_data_py` Python package and establish a data lake where the data is stored and made accessible for various use cases like analytics and machine learning.

## Core Technologies

- **Environment & Dependencies:** `uv` for Python environments, `ansible` for setup automation.
- **Data Source:** `nfl_data_py` Python package.
- **Data Orchestration:** `dagster`.
- **Data Transformation:** `dbt`.
- **Data Lake:** `ducklake` (DuckDB with Parquet files and a Postgres metadata store).
- **Containerization:** `podman` and `docker-compose`.
- **CLI Tool:** `click`.
- **Code Quality:** `ruff` for linting and formatting.

## Project Phases

### Phase 1: Project Setup and Exploration CLI (Completed)

**Summary:** This phase established the foundational infrastructure for the project. An Ansible playbook was created to automate the setup of a `uv`-managed Python environment. A command-line interface (CLI) was developed using `click` to enable exploration of the `nfl_data_py` library and to inspect Parquet files. The project structure was created following Dagster best practices, and the entire CLI tool is covered by unit tests.

1.  **Environment Setup:**
    -   Use `ansible` to create playbooks for setting up development and production environments.
    -   Ensure `uv` is used for all Python package and virtual environment management.
    -   Install and configure `ruff` for code quality.
2.  **Project Scaffolding:**
    -   Create the main directory structure following `dagster` best practices.
    -   Set up the `.gitignore` file to exclude sensitive information and environment-specific files.
3.  **Exploration CLI:**
    -   Develop a CLI tool using `click`.
    -   Implement a command to fetch and display data from `nfl_data_py` to the console for exploration.
    -   Implement a command to read and display data from Parquet files.
4.  **Testing:**
    -   Write and execute unit tests for all CLI functionalities.
5.  **Verification:**
    -   Demonstrate the working CLI by showing data extraction and reading from the console.

### Phase 2: Data Extraction Pipeline

1.  **Configuration:**
    -   Create configuration files for each dataset available in `nfl_data_py`, specifying parameters like starting years.
2.  **Extraction Logic:**
    -   Develop functions to extract data for each dataset by year.
    -   Save the extracted data in Parquet format.
    -   Partition the data by `etl_date` to manage incremental updates.
3.  **Dagster Integration:**
    -   Define Dagster assets for each dataset to orchestrate the extraction process.
4.  **Testing:**
    -   Write and execute tests for the data extraction functions and Dagster assets.

### Phase 3: Data Transformation with dbt

1.  **dbt Project Setup:**
    -   Initialize a `dbt` project within the repository.
    -   Integrate the `dbt` project with `dagster`.
2.  **dbt Models:**
    -   **Staging Models:** Create models for basic cleaning and type casting of the raw data.
    -   **Intermediate Models:** Develop models to aggregate or split data, such as by team or player position.
    -   **Final Models:** Build mart layers suitable for consumption by BI tools or ML models.
3.  **Documentation & Testing:**
    -   Create `dbt` documentation for all models and schemas.
    -   Write tests for all `dbt` models to ensure data quality.

### Future Phases (Pending)

-   Development of ML models based on the transformed data.
-   Creation of dashboards for data visualization.
-   Advanced data quality monitoring and alerting.
