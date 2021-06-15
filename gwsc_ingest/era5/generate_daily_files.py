import logging
import os.path
import sys

import xarray as xr

from gwsc_ingest.utils.logging import setup_basic_logging

log = logging.getLogger(__name__)


def generate_daily_files(in_filename, out_filename):
    log.info(f'Processing file: {in_filename}')

    # Open file
    with xr.load_dataset(in_filename) as ds:
        # Convert temperature from K to C
        ds['t2m_c'] = ds.t2m - 273.15
        ds['t2m_c'].attrs['long_name'] = ds.t2m.long_name.replace('metre', 'meter')
        ds['t2m_c'].attrs['units'] = 'C'

        # Convert precipitation from meters to millimeters
        ds['tp_mm'] = ds.tp * 1000
        ds['tp_mm'].attrs['long_name'] = ds.tp.long_name
        ds['tp_mm'].attrs['units'] = 'mm'

        # Compute mean temperature
        mean_t2m_c = ds.t2m_c.mean('time')
        mean_t2m_c.attrs['long_name'] = 'Mean ' + ds.t2m_c.long_name
        mean_t2m_c.attrs['units'] = 'C'
        log.debug(f'\n----------Mean Temperature @ 2 Meters----------\n{mean_t2m_c}')

        # Compute minimum temperature
        min_t2m_c = ds.t2m_c.min('time')
        min_t2m_c.attrs['long_name'] = 'Minimum ' + ds.t2m_c.long_name
        min_t2m_c.attrs['units'] = 'C'
        log.debug(f'\n----------Min. Temperature @ 2 Meters----------\n{min_t2m_c}')

        # Compute maximum temperature
        max_t2m_c = ds.t2m_c.max('time')
        max_t2m_c.attrs['long_name'] = 'Maximum ' + ds.t2m_c.long_name
        max_t2m_c.attrs['units'] = 'C'
        log.debug(f'\n----------Max. Temperature @ 2 Meters----------\n{max_t2m_c}')

        # Compute total precipitation
        sum_tp_mm = ds.tp_mm.sum('time')
        sum_tp_mm.attrs['long_name'] = ds.tp_mm.long_name
        sum_tp_mm.attrs['units'] = 'mm'
        log.debug(f'\n----------Total Precipitation @ Surface ----------\n{sum_tp_mm}')

        # Create new Dataset with all summary variables
        out_ds = xr.Dataset({
            'mean_t2m_c': mean_t2m_c,
            'max_t2m_c': max_t2m_c,
            'min_t2m_c': min_t2m_c,
            'sum_tp_mm': sum_tp_mm
        })
        log.debug(out_ds)

        # Write out to netcdf file
        if os.path.isdir(out_filename):
            # Automatically generate out filename with date
            file_date = ds.time[0].dt.strftime('%Y-%m-%d').item()
            log.debug(file_date)
            out_filename = os.path.join(out_filename, f'reanalysis-era5-sfc-daily-{file_date}.nc')
            log.info(f'Writing results to: {out_filename} ...')

        out_ds.to_netcdf(out_filename)


if __name__ == '__main__':
    setup_basic_logging(logging.DEBUG)
    args = sys.argv[1:]
    log.debug(args)
    generate_daily_files(*args)
