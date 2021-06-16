import argparse
import datetime as dt
from pathlib import Path


def validate_directory(directory, arg_name="directory"):
    """
    Verify argument is path to existing directory and is provided in as a string or pathlib.Path object.

    Args:
        directory (str or pathlib.Path): Path to an existing directory.
        arg_name (str): Name of the argument to use in ValueError messages.

    Returns:
        pathlib.Path: path to validated directory as a pathlib.Path object.
    """
    if isinstance(directory, str):
        directory = Path(directory)
    elif not isinstance(directory, Path):
        raise ValueError(f'"{arg_name}" must be a string or pathlib.Path object.')
    if not directory.is_dir():
        raise ValueError(f'"{arg_name}" must be a path to an existing directory. '
                         f'No such directory found: {str(directory)}')
    return directory


def validate_date_string(s, f="%Y-%m-%d"):
    """
    Validate a given date string for a give date format.
    Args:
        s (str): a string formatted date.
        f (str): date format to validate. Defaults to "%Y-%m-%d".

    Raises:
        argparse.ArgumentTypeError: when invalid date string.
    """
    try:
        return dt.datetime.strptime(s, f)
    except ValueError:
        msg = f'Not a valid date: "{s}"'
        raise argparse.ArgumentTypeError(msg)
