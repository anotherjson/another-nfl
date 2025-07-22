"""dbt resource for Dagster pipeline."""

import subprocess
from pathlib import Path
from dagster import ConfigurableResource, get_dagster_logger


class DbtResource(ConfigurableResource):
    """dbt resource for running transformations."""
    
    project_dir: str = "../dbt"
    profiles_dir: str = "../dbt"
    target: str = "dev"
    
    def run_dbt_command(self, command: list[str]) -> dict:
        """Run a dbt command and return the result."""
        logger = get_dagster_logger()
        
        full_command = ["uv", "run", "dbt"] + command
        
        logger.info(f"Running dbt command: {' '.join(full_command)}")
        
        try:
            result = subprocess.run(
                full_command,
                capture_output=True,
                text=True,
                check=True,
                cwd=Path(__file__).parent.parent.parent / "dbt"  # dbt directory
            )
            
            logger.info(f"dbt command succeeded: {result.stdout}")
            return {
                "success": True,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "returncode": result.returncode
            }
            
        except subprocess.CalledProcessError as e:
            logger.error(f"dbt command failed: {e.stderr}")
            return {
                "success": False,
                "stdout": e.stdout,
                "stderr": e.stderr,
                "returncode": e.returncode
            }
    
    def run(self, select: str = None) -> dict:
        """Run dbt models."""
        command = ["run"]
        if select:
            command.extend(["--select", select])
        return self.run_dbt_command(command)
    
    def test(self, select: str = None) -> dict:
        """Run dbt tests."""
        command = ["test"]
        if select:
            command.extend(["--select", select])
        return self.run_dbt_command(command)
    
    def compile(self) -> dict:
        """Compile dbt models."""
        return self.run_dbt_command(["compile"])
    
    def docs_generate(self) -> dict:
        """Generate dbt documentation."""
        return self.run_dbt_command(["docs", "generate"])


# Resource instance
dbt_resource = DbtResource()