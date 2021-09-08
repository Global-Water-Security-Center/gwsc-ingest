import datetime as dt
import logging

from gwsc_ingest.era5.download import bulk_download_one_day_ran_sfc
from gwsc_ingest.utils.logging import setup_basic_logging
from gwsc_ingest.utils.validation import validate_directory

log = logging.getLogger(__name__)


def check_and_download(dir_with_files, download_missing=False, processes=1, api_key=None):
    """
    Check for missing files and download missing files if desired.

    Args:
        dir_with_files (path): Path to directory with files to verify.
        download_missing (bool): Download missing files if True. Defaults to False.
        processes (int): Number of concurrent processes to use to download the data.
        api_key (str): CDS API Key. Attempts to read from ~/.cdsapirc file if not provided.
    """
    missing_dates = check_for_missing_files(dir_with_files=dir_with_files, return_dates=True)
    log.debug(f'Missing Dates: {missing_dates}')

    if len(missing_dates) <= 0:
        log.info("No missing files found.")
        return

    if download_missing:
        log.info(f'Downloading {len(missing_dates)} files...')
        bulk_download_one_day_ran_sfc(
            days=missing_dates,
            download_dir=dir_with_files,
            processes=processes,
            api_key=api_key
        )


def check_for_missing_files(dir_with_files, return_dates=False):
    """
    Checks for missing files after a bulk download.

    Args:
        dir_with_files (path): Path to directory with files to verify.
        return_dates (bool): Return list of dates if True. Otherwise return list of Paths.

    Returns:
        list: missing files/dates
    """
    dir_with_files = validate_directory(dir_with_files, 'download_dir')
    log.info(f'Checking files in directory: {dir_with_files}')

    files_found = sorted(dir_with_files.iterdir())
    log.info(f'Number of Files: {len(files_found)}')

    first_file = files_found[0]
    last_file = files_found[-1]
    log.info(f'First File: {first_file}')
    log.info(f'Last File: {last_file}')

    date_format_str = 'reanalysis-era5-single-levels-24-hours-%Y-%m-%d'
    first_date = dt.datetime.strptime(first_file.stem, date_format_str)
    last_date = dt.datetime.strptime(last_file.stem, date_format_str)
    log.info(f'First Date: {first_date}')
    log.info(f'Last Date: {last_date}')

    curr_date = first_date
    expected_files = set()
    while curr_date <= last_date:
        expected_files.add(dir_with_files / f'{curr_date.strftime(date_format_str)}.nc')
        curr_date += dt.timedelta(days=1)

    files_found = set(files_found)
    diff_files = expected_files.difference(files_found)
    log.info(f'Number of Missing Files: {len(diff_files)}')
    sorted_dif_files = sorted(diff_files)
    log.debug(f'Missing Files: {sorted_dif_files}')

    if not return_dates:
        return diff_files

    else:
        return [dt.datetime.strptime(p.stem, date_format_str) for p in diff_files]


def _verify_command(args):
    log_level = logging.DEBUG if args.debug else logging.INFO
    setup_basic_logging(log_level)
    log.debug(f'Given arguments: {args}')
    check_and_download(
        dir_with_files=args.dir,
        download_missing=args.download_missing,
        processes=args.processes,
        api_key=args.key,
    )


def _add_verify_parser_arguments(parser):
    parser.add_argument("dir", help="Path to directory with files to verify.")
    parser.add_argument("-d", "--download-missing", dest="download_missing", action='store_true',
                        help="Download missing files.")
    parser.add_argument("-p" "--processes", dest="processes", type=int, required=False, default=1,
                        help="Number of concurrent processes to use to download the files.")
    parser.add_argument("-k" "--key", dest="key", type=str, required=False, default=None,
                        help="CDS API Key")
    parser.add_argument("-d" "--debug", dest="debug", action='store_true',
                        help="Turn on debug logging.")
    parser.set_defaults(func=_verify_command)


def _add_verify_parser(subparsers):
    p = subparsers.add_parser(
        'era5-verify',
        description="Checks for missing files after a bulk download "
                    "and verifies files are expected size."
    )
    _add_verify_parser_arguments(p)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description="Checks for missing files after a bulk download "
                    "and verifies files are expected size."
    )
    _add_verify_parser_arguments(parser)
    args = parser.parse_args()
    args.func(args)
