import datetime as dt
import logging
from pathlib import Path

import cdsapi

from gwsc_ingest.utils.logging import setup_basic_logging

log = logging.getLogger(__name__)


def download_one_day_ran_sfc(day, download_dir, download_format='netcdf'):
    """
    Downloads 24 hours of the reanalysis-era5-single-levels (ran-sfc) data for the given day.

    Args:
        day (datetime.datetime): Day to download. Datetime object with year, month, and day defined.
        download_dir (path): Path to directory where data will be downloaded to.
        download_format (str): Format data will be downloaded as. One of "netcdf" or "grib". Defaults to "netcdf".
    """
    # Validate input
    if not isinstance(day, dt.datetime):
        raise ValueError('"day" must be a datetime.datetime object.')

    if isinstance(download_dir, str):
        download_dir = Path(download_dir)
    elif not isinstance(download_dir, Path):
        raise ValueError('"download_dir" must be a string or pathlib.Path object.')

    if not download_dir.is_dir():
        raise ValueError('"download_dir" must be a path to an existing directory. No such directory found: ')

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

    # Submit the download request using the CDS Python API
    cds = cdsapi.Client()
    r = cds.retrieve(
        'reanalysis-era5-single-levels',
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


if __name__ == '__main__':
    setup_basic_logging(logging.INFO)
    download_one_day_ran_sfc(
        day=dt.datetime(year=2021, month=6, day=5),
        download_dir='/home/gwscdev/aquadev/era5/daily_hourly'
    )
