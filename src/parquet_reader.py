"""Parquet file reader module for exploring parquet data files."""

from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq


class ParquetReader:
    """Handles reading and exploring parquet files."""

    def read_parquet(self, file_path: Path, limit: int = 10) -> pd.DataFrame:
        """
        Read parquet file and return specified number of rows.

        Args:
            file_path: Path to the parquet file
            limit: Number of rows to return

        Returns:
            DataFrame containing the data

        Raises:
            FileNotFoundError: If file doesn't exist
            Exception: If reading fails
        """
        try:
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")

            # Read the parquet file
            df = pd.read_parquet(file_path)

            if limit > 0:
                return df.head(limit)
            else:
                return df

        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {file_path}")
        except Exception as e:
            raise Exception(f"Failed to read parquet file {file_path}: {str(e)}")

    def get_file_info(self, file_path: Path) -> dict[str, Any]:
        """
        Get information about a parquet file.

        Args:
            file_path: Path to the parquet file

        Returns:
            Dictionary containing file information

        Raises:
            FileNotFoundError: If file doesn't exist
            Exception: If reading metadata fails
        """
        try:
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")

            # Get file stats
            file_stats = file_path.stat()

            # Read parquet metadata
            parquet_file = pq.ParquetFile(file_path)
            metadata = parquet_file.metadata
            schema = parquet_file.schema

            info = {
                "File Name": file_path.name,
                "File Size (MB)": round(file_stats.st_size / (1024 * 1024), 2),
                "Modified": pd.Timestamp(file_stats.st_mtime, unit="s").strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "Total Rows": metadata.num_rows,
                "Total Columns": len(schema),
                "Row Groups": metadata.num_row_groups,
                "Columns": [field.name for field in schema],
                "Column Types": {
                    field.name: str(field.physical_type) for field in schema
                },
            }

            return info

        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {file_path}")
        except Exception as e:
            raise Exception(f"Failed to get file info for {file_path}: {str(e)}")

    def get_column_stats(self, file_path: Path) -> dict[str, Any]:
        """
        Get statistical information about columns in the parquet file.

        Args:
            file_path: Path to the parquet file

        Returns:
            Dictionary containing column statistics

        Raises:
            Exception: If reading fails
        """
        try:
            df = pd.read_parquet(file_path)

            stats = {
                "shape": df.shape,
                "memory_usage_mb": round(
                    df.memory_usage(deep=True).sum() / (1024 * 1024), 2
                ),
                "dtypes": df.dtypes.to_dict(),
                "null_counts": df.isnull().sum().to_dict(),
                "null_percentages": (df.isnull().sum() / len(df) * 100)
                .round(2)
                .to_dict(),
            }

            # Add numeric column statistics
            numeric_cols = df.select_dtypes(include=["number"]).columns
            if len(numeric_cols) > 0:
                stats["numeric_stats"] = df[numeric_cols].describe().to_dict()

            return stats

        except Exception as e:
            raise Exception(f"Failed to get column stats for {file_path}: {str(e)}")
