import datetime as dt
import logging
import multiprocessing as mp

import cdsapi
import humanize

from gwsc_ingest.utils.logging import setup_basic_logging
from gwsc_ingest.utils.validation import validate_directory

_COMMAND_DESCRIPTION = "Downloads multiple 24 hour reanalysis-era5-single-levels (ran-sfc) " \
                       "datasets, one for each day in the given date range."
log = logging.getLogger(__name__)


def bulk_download_one_day_ran_sfc(days, download_dir, download_format='netcdf', processes=1, api_key=None):
    """
    Downloads multiple 24 hour reanalysis-era5-single-levels (ran-sfc) datasets,
        one for each day in the given date range.

    Args:
        days (list<datetime.datetime>): Days to download as a list of datetime object with year, month, and day defined.
        download_dir (path): Path to directory where data will be downloaded to.
        download_format (str): Format data will be downloaded as: one of "netcdf" or "grib". Defaults to "netcdf".
        processes (int): Number of concurrent processes to use to download the data.
        api_key (str): CDS API Key. Attempts to read from ~/.cdsapirc file if not provided.
    """
    # Validate input
    if processes < 1:
        log.warning('"processes" should not be negative. Using to 1 process.')
        processes = 1

    # Save current time for timing
    start_time = dt.datetime.utcnow()

    # Build task arguments
    tasks = []
    for day in days:
        if not isinstance(day, dt.datetime):
            log.warning(f'Invalid day given: {day}. Must be a datetime.datetime object. Skipping...')
        tasks.append((day, download_dir, download_format, api_key))

    # Execute
    with mp.Pool(processes=processes) as p:
        # This will block until finished
        p.starmap(download_one_day_ran_sfc, tasks)

    # Report time taken
    time_to_download = dt.datetime.utcnow() - start_time
    log.info(f'All downloads completed in {humanize.precisedelta(time_to_download)}')


def download_one_day_ran_sfc(day, download_dir, download_format='netcdf', api_key=None):
    """
    Downloads 24 hours of the reanalysis-era5-single-levels (ran-sfc) data for the given day.

    Args:
        day (datetime.datetime): Day to download. Datetime object with year, month, and day defined.
        download_dir (path): Path to directory where data will be downloaded to.
        download_format (str): Format data will be downloaded as: one of "netcdf" or "grib". Defaults to "netcdf".
    """
    # Validate input
    if not isinstance(day, dt.datetime):
        raise ValueError('"day" must be a datetime.datetime object.')

    download_dir = validate_directory(download_dir, 'download_dir')

    if download_format not in ['netcdf', 'grib']:
        raise ValueError('"download_format" must be one of "netcdf" or "grib".')

    # Derive request params
    year_str = day.strftime('%Y')
    month_str = day.strftime('%m')
    day_str = day.strftime('%d')
    log.info(f'Retrieving 24 hours of data for date (Y-M-D): {year_str}-{month_str}-{day_str}')

    # Define output filename and location
    out_file_ext = 'nc' if download_format == 'netcdf' else 'grib'
    out_filename = f'reanalysis-era5-single-levels-24-hours-{day:%Y-%m-%d}.{out_file_ext}'
    out_path = download_dir / out_filename

    # Get start time for timing
    start_time = dt.datetime.utcnow()

    # Submit the download request using the CDS Python API
    if api_key is None:
        cds = cdsapi.Client()
    else:
        cds = cdsapi.Client(
            url='https://cds.climate.copernicus.eu/api/v2',
            key=api_key,
        )

    if day > dt.datetime(1978, 12, 31):
        dataset_name = 'reanalysis-era5-single-levels'
    else:
        dataset_name = 'reanalysis-era5-single-levels-preliminary-back-extension'

    r = cds.retrieve(
        dataset_name,
        {
            'product_type': 'reanalysis',
            'variable': [
                '2m_temperature',
                'total_precipitation',
            ],
            'year': year_str,
            'month': [month_str],
            'day': [day_str],
            'time': [
                '00:00', '01:00', '02:00', '03:00', '04:00', '05:00',
                '06:00', '07:00', '08:00', '09:00', '10:00', '11:00',
                '12:00', '13:00', '14:00', '15:00', '16:00', '17:00',
                '18:00', '19:00', '20:00', '21:00', '22:00', '23:00',
            ],
            'format': download_format,
        },
        out_path
    )
    log.debug(r)

    time_to_download = dt.datetime.utcnow() - start_time
    log.info(f'Downloaded file: {out_path}')
    log.info(f'Download completed in {humanize.precisedelta(time_to_download)}')


def _download_command(args):
    log_level = logging.DEBUG if args.debug else logging.INFO
    setup_basic_logging(log_level)
    log.debug(f'Given arguments: {args}')
    last_day = args.last_day
    days = [last_day - dt.timedelta(days=x) for x in range(args.num_days)]
    bulk_download_one_day_ran_sfc(
        days=days,
        download_dir=args.download_dir,
        processes=args.processes,
        api_key=args.key,
    )


def _add_download_parser_arguments(parser):
    from gwsc_ingest.utils.validation import validate_date_string

    parser.add_argument("last_day", type=validate_date_string,
                        help="The last day of data to download in YYYY-MM-DD format.")
    parser.add_argument("num_days", type=int,
                        help="Number of days before last_day to retrieve.")
    parser.add_argument("download_dir",
                        help="Path to directory where data will be downloaded to.")
    parser.add_argument("-p" "--processes", dest="processes", type=int, required=False, default=1,
                        help="Number of concurrent processes to use to download the files.")
    parser.add_argument("-k" "--key", dest="key", type=str, required=False, default=None,
                        help="The CDS UID and API Key for authentication provided in the following "
                             "format: <UID>:<API_KEY>.")
    parser.add_argument("-d" "--debug", dest="debug", action='store_true',
                        help="Turn on debug logging.")
    parser.set_defaults(func=_download_command)


def _add_download_parser(subparsers):
    p = subparsers.add_parser('era5-download', description=_COMMAND_DESCRIPTION)
    _add_download_parser_arguments(p)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=_COMMAND_DESCRIPTION)
    _add_download_parser_arguments(parser)
    args = parser.parse_args()
    args.func(args)
