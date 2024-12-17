import yaml
from logging import Logger
from pathlib import Path

def get_yaml_files(paths: str) -> list[str]:
    """
    Iterate through a comma-separated string of files and dirs. Collect YAML files from each.

    Args:
        paths (str): Comma-separated string of files and dirs.
    
    Returns:
        list: List of YAML file paths.
    """
    yaml_files = []

    for path in paths.split(','):
        path = Path(path.strip())
        if path.is_file() and path.suffix == '.yaml':
            yaml_files.append(path)
        elif path.is_dir():
            yaml_files.extend(path.rglob('*.yaml'))

    return yaml_files

def parse_yaml_files(logger: Logger, files):
    yaml_data = {}
    try:
        for file in files:
            with open(file, 'r') as f:
                # Assuming the file contains text, read and process it
                data = yaml.safe_load(f)

                yaml_data[file] = data
    except Exception as e:
        logger.error(f"Could not parse YAML: {e}")

    return yaml_data