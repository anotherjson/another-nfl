"""
Configuration loader for NFL dataset extraction parameters.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass
class DatasetConfig:
    """Configuration for a specific NFL dataset."""

    name: str
    description: str
    function_name: str
    start_year: int | None
    requires_year: bool
    data_type: str
    partition_by: str
    validation: dict[str, Any]
    extraction: dict[str, Any]
    output: dict[str, Any]


class ConfigLoader:
    """Loads and validates NFL dataset configurations."""

    def __init__(self, config_dir: Path | None = None):
        """
        Initialize configuration loader.

        Args:
            config_dir: Path to configuration directory. Defaults to configs/datasets
        """
        if config_dir is None:
            # Default to configs/datasets relative to project root
            project_root = Path(__file__).parent.parent
            config_dir = project_root / "configs" / "datasets"

        self.config_dir = Path(config_dir)
        self._validate_config_dir()
        self._configs: dict[str, DatasetConfig] = {}
        self._load_all_configs()

    def _validate_config_dir(self) -> None:
        """Validate that the configuration directory exists."""
        if not self.config_dir.exists():
            raise FileNotFoundError(
                f"Configuration directory not found: {self.config_dir}"
            )

        if not self.config_dir.is_dir():
            raise NotADirectoryError(
                f"Config path is not a directory: {self.config_dir}"
            )

    def _load_all_configs(self) -> None:
        """Load all YAML configuration files from the config directory."""
        yaml_files = list(self.config_dir.glob("*.yaml"))

        if not yaml_files:
            raise ValueError(f"No YAML configuration files found in: {self.config_dir}")

        for config_file in yaml_files:
            try:
                config = self._load_single_config(config_file)
                self._configs[config.name] = config
            except Exception as e:
                raise Exception(f"Failed to load config {config_file}: {str(e)}")

    def _load_single_config(self, config_file: Path) -> DatasetConfig:
        """Load and validate a single configuration file."""
        try:
            with open(config_file) as f:
                config_data = yaml.safe_load(f)
        except Exception as e:
            raise Exception(f"Failed to parse YAML file {config_file}: {str(e)}")

        # Validate required fields
        required_fields = [
            "name",
            "description",
            "function_name",
            "requires_year",
            "data_type",
            "partition_by",
            "validation",
            "extraction",
            "output",
        ]

        missing_fields = [
            field for field in required_fields if field not in config_data
        ]
        if missing_fields:
            raise ValueError(
                f"Missing required fields in {config_file}: {missing_fields}"
            )

        # Validate specific field values
        self._validate_config_data(config_data, config_file)

        return DatasetConfig(**config_data)

    def _validate_config_data(
        self, config_data: dict[str, Any], config_file: Path
    ) -> None:
        """Validate configuration data values."""
        # Validate partition_by values
        valid_partition_types = ["year", "etl_date_only"]
        if config_data["partition_by"] not in valid_partition_types:
            raise ValueError(
                f"Invalid partition_by in {config_file}: {config_data['partition_by']}. "
                f"Must be one of {valid_partition_types}"
            )

        # Validate requires_year consistency
        if config_data["requires_year"] and config_data["start_year"] is None:
            raise ValueError(
                f"Config {config_file}: If requires_year is True, start_year must be specified"
            )

        # Validate validation section
        validation = config_data.get("validation", {})
        if "required_columns" not in validation:
            raise ValueError(
                f"Missing 'required_columns' in validation section of {config_file}"
            )

        if not isinstance(validation["required_columns"], list):
            raise ValueError(f"'required_columns' must be a list in {config_file}")

        # Validate extraction section
        extraction = config_data.get("extraction", {})
        if "timeout_seconds" not in extraction:
            raise ValueError(
                f"Missing 'timeout_seconds' in extraction section of {config_file}"
            )

        # Validate output section
        output = config_data.get("output", {})
        required_output_fields = ["file_format", "compression", "path_template"]
        missing_output_fields = [
            field for field in required_output_fields if field not in output
        ]
        if missing_output_fields:
            raise ValueError(
                f"Missing required output fields in {config_file}: {missing_output_fields}"
            )

    def get_dataset_config(self, dataset_name: str) -> DatasetConfig:
        """
        Get configuration for a specific dataset.

        Args:
            dataset_name: Name of the dataset

        Returns:
            DatasetConfig for the specified dataset

        Raises:
            KeyError: If dataset configuration not found
        """
        if dataset_name not in self._configs:
            available_datasets = list(self._configs.keys())
            raise KeyError(
                f"Dataset '{dataset_name}' not found. "
                f"Available datasets: {sorted(available_datasets)}"
            )

        return self._configs[dataset_name]

    def list_datasets(self) -> list[str]:
        """
        Get list of all available dataset names.

        Returns:
            Sorted list of dataset names
        """
        return sorted(self._configs.keys())

    def get_datasets_by_type(self, data_type: str) -> list[str]:
        """
        Get list of datasets of a specific type.

        Args:
            data_type: Type of datasets to retrieve

        Returns:
            List of dataset names matching the type
        """
        return [
            name
            for name, config in self._configs.items()
            if config.data_type == data_type
        ]

    def get_datasets_requiring_year(self) -> list[str]:
        """
        Get list of datasets that require year parameter.

        Returns:
            List of dataset names requiring year
        """
        return [name for name, config in self._configs.items() if config.requires_year]

    def get_datasets_by_start_year(self, min_year: int) -> list[str]:
        """
        Get list of datasets that have data available from a minimum year.

        Args:
            min_year: Minimum year to filter by

        Returns:
            List of dataset names with data from min_year or later
        """
        return [
            name
            for name, config in self._configs.items()
            if config.start_year is None or config.start_year <= min_year
        ]

    def validate_year_for_dataset(self, dataset_name: str, year: int) -> bool:
        """
        Validate if a year is valid for a specific dataset.

        Args:
            dataset_name: Name of the dataset
            year: Year to validate

        Returns:
            True if year is valid for the dataset

        Raises:
            KeyError: If dataset configuration not found
            ValueError: If year is invalid for the dataset
        """
        config = self.get_dataset_config(dataset_name)

        if not config.requires_year:
            return True

        if config.start_year is not None and year < config.start_year:
            raise ValueError(
                f"Year {year} is invalid for dataset '{dataset_name}'. "
                f"Data starts from year {config.start_year}."
            )

        return True

    def get_output_path(
        self,
        dataset_name: str,
        year: int | None = None,
        etl_date: str | None = None,
    ) -> str:
        """
        Generate output path for a dataset based on its configuration.

        Args:
            dataset_name: Name of the dataset
            year: Year for partitioning (if required)
            etl_date: ETL date for partitioning

        Returns:
            Generated file path string
        """
        config = self.get_dataset_config(dataset_name)
        path_template = config.output["path_template"]

        # Default etl_date if not provided
        if etl_date is None:
            import datetime as dt

            etl_date = dt.datetime.now().strftime("%Y-%m-%d")

        # Replace template variables
        replacements = {"dataset": dataset_name, "etl_date": etl_date}

        if year is not None:
            replacements["year"] = str(year)

        try:
            return path_template.format(**replacements)
        except KeyError as e:
            raise ValueError(
                f"Missing template variable {e} for dataset {dataset_name}"
            )
