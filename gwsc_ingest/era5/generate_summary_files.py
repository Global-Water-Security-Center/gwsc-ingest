import datetime as dt
import logging
import multiprocessing as mp
import os.path

import humanize
import numpy as np
from tqdm import tqdm
import xarray as xr

from gwsc_ingest.utils.logging import setup_basic_logging
from gwsc_ingest.utils.validation import validate_directory

log = logging.getLogger(__name__)


def bulk_generate_summary_files(in_directory, out_directory, processes=1):
    """
    Process a directory of ERA5 NetCDF files containing total precipitation (tp) and
        2-meter temperature (t2m) data into summary files.

    Args:
        in_directory (path): Path to directory containing ERA5 NetCDF files with tp and t2m variables.
        out_directory (path): Path to directory where summary files will be written to.
        processes (int): Number of concurrent processes to use to process the files.
    """
    # Validate input
    in_directory = validate_directory(in_directory, 'in_directory')
    out_directory = validate_directory(out_directory, 'out_directory')

    # Save current time for timing
    start_time = dt.datetime.utcnow()

    # Build task arguments
    tasks = []
    for item in in_directory.iterdir():
        if item.is_file() and 'nc' in item.suffix:
            tasks.append({
                'in_filename': str(item),
                'out_filename': str(out_directory)}
            )

    log.debug(tasks)
    log.debug(f'Num tasks: {len(tasks)}')

    # Execute
    with mp.Pool(processes=processes) as p:
        # This will block until finished
        for _ in tqdm(p.imap_unordered(_worker, tasks), total=len(tasks)):
            pass

    # Report time taken
    time_to_download = dt.datetime.utcnow() - start_time
    log.info(f'Processed {len(tasks)} files in {humanize.precisedelta(time_to_download)} '
             f'using {processes} processes.')


def _worker(qwargs):
    """
    Worker function that parses out kwargs (dict) and calls generate_summary_files with them.

    Args:
        qwargs (dict): Keyword arguments of generate_summary_files.
    """
    generate_summary_files(
        in_filename=qwargs.get('in_filename'),
        out_filename=qwargs.get('out_filename'),
    )


def generate_summary_files(in_filename, out_filename):
    """
    Generate a summary file from an ERA5 NetCDF file containing total precipitation (tp) and 2-meter
        temperature (t2m) data. Summary file will contain the following variables: Minimum, Mean,
        and Maximum 2-meter temperature (min_t2m_c, mean_t2m_c, max_t2m_c) and Total precipitation
        (summed over the entire period; sum_tp_mm).

    Args:
        in_filename (str): Path to NetCDF file of ERA5 data with total precipitation (tp) and 2-meter
            temperature (t2m) variables.
        out_filename (str): Path to file or directory where summary file will be written to. If directory
            is provided, the following naming convention will be applied using the first date in the file:
            reanalysis-era5-sfc-daily-%Y-%m-%d.nc
    """
    log.info(f'Processing file: {in_filename}')

    # Derive out filename
    file_date = None
    if os.path.isdir(out_filename):
        # Automatically generate out filename with date
        file_date = dt.datetime.strptime(
            os.path.basename(in_filename),
            'reanalysis-era5-single-levels-24-hours-%Y-%m-%d.nc'
        )
        log.debug(file_date)
        out_filename = os.path.join(out_filename, f'reanalysis-era5-sfc-daily-{file_date:%Y-%m-%d}.nc')
        log.info(f'Writing results to: {out_filename} ...')

    # Check to see if given filename/generated filename is an existing file
    if os.path.isfile(out_filename):
        log.warning(f'A summary file with name "{out_filename}" already exists. Skipping...')
        return

    # Open file
    with xr.load_dataset(in_filename) as ds:
        # Datasets with expver dimension have both ERA5 and ERA5T data
        # ERA5 Data is assigned experiment version (expver) 1, while ERA5T expver is 5
        # See: https://confluence.ecmwf.int/display/CUSF/ERA5+CDS+requests+which+return+a+mixture+of+ERA5+and+ERA5T+data
        if 'expver' in ds.dims:
            log.warning(f'\nDataset "{in_filename}" contains both ERA5 and ERA5T data. Resulting dataset will be '
                        f'a combination of both. See: https://confluence.ecmwf.int/display/CUSF/'
                        f'ERA5+CDS+requests+which+return+a+mixture+of+ERA5+and+ERA5T+data')

            # Temperature is from expver 5 (ERA5T)
            new_ds = ds.sel(expver=5)

            # But the first 6 hours of precip are from expver 1 (ERA5)
            precip_expver1 = ds.tp.sel(expver=1).isel(time=slice(None, 7))  # ERA5 portion
            precip_expver5 = ds.tp.sel(expver=5).isel(time=slice(7, None))  # ERA5T portion
            new_ds['tp'] = xr.concat([precip_expver1, precip_expver5], 'time')
            ds = new_ds

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
        mean_t2m_c = add_time_dimension(mean_t2m_c, file_date)
        log.debug(f'\n----------Mean Temperature @ 2 Meters----------\n{mean_t2m_c}')

        # Compute minimum temperature
        min_t2m_c = ds.t2m_c.min('time')
        min_t2m_c.attrs['long_name'] = 'Minimum ' + ds.t2m_c.long_name
        min_t2m_c.attrs['units'] = 'C'
        min_t2m_c = add_time_dimension(min_t2m_c, file_date)
        log.debug(f'\n----------Min. Temperature @ 2 Meters----------\n{min_t2m_c}')

        # Compute maximum temperature
        max_t2m_c = ds.t2m_c.max('time')
        max_t2m_c.attrs['long_name'] = 'Maximum ' + ds.t2m_c.long_name
        max_t2m_c.attrs['units'] = 'C'
        max_t2m_c = add_time_dimension(max_t2m_c, file_date)
        log.debug(f'\n----------Max. Temperature @ 2 Meters----------\n{max_t2m_c}')

        # Compute total precipitation
        sum_tp_mm = ds.tp_mm.sum('time')
        sum_tp_mm.attrs['long_name'] = ds.tp_mm.long_name
        sum_tp_mm.attrs['units'] = 'mm'
        sum_tp_mm = add_time_dimension(sum_tp_mm, file_date)
        log.debug(f'\n----------Sum of Total Precipitation @ Surface ----------\n{sum_tp_mm}')

        # Create new Dataset with all summary variables
        out_ds = xr.Dataset({
            'mean_t2m_c': mean_t2m_c,
            'max_t2m_c': max_t2m_c,
            'min_t2m_c': min_t2m_c,
            'sum_tp_mm': sum_tp_mm
        })
        log.debug(out_ds)

        out_ds.to_netcdf(out_filename)


def add_time_dimension(data_array, time):
    """
    Creates a new xr.DataArray with a time dimension with a single time.

    Args:
        data_array (xr.DataArray): A 2D xr.DataArray with latitude and longitude coordinates.
        time (datetime): A single datetime object.

    Returns:
        xr.DataArray: The new array with the time dimension added.
    """
    data_array = xr.DataArray(
        np.array([data_array.data.copy()]),
        coords=[
            ('time', np.array([time])),
            ('latitude', data_array.latitude.data.copy()),
            ('longitude', data_array.longitude.data.copy())
        ],
        attrs=data_array.attrs,
    )
    return data_array


def _generate_summary_command(args):
    log_level = logging.DEBUG if args.debug else logging.INFO
    setup_basic_logging(log_level)
    log.debug(f'Given arguments: {args}')
    bulk_generate_summary_files(args.in_directory, args.out_directory, args.processes)


def _add_generate_summary_parser_arguments(parser):
    parser.add_argument("in_directory",
                        help="Path to directory containing ERA5 NetCDF files with tp and t2m variables.")
    parser.add_argument("out_directory",
                        help="Path to directory where summary files will be written to.")
    parser.add_argument("-p" "--processes", dest="processes", type=int, required=False, default=1,
                        help="Number of concurrent processes to use to process the files.")

    parser.add_argument("-d" "--debug", dest="debug", action='store_true',
                        help="Turn on debug logging.")
    parser.set_defaults(func=_generate_summary_command)


def _add_generate_summary_parser(subparsers):
    p = subparsers.add_parser(
        'era5-gen-daily-ds',
        description="Process a directory of ERA5 NetCDF files containing total "
                    "precipitation (tp) and 2-meter temperature (t2m) data into "
                    "summary files."
    )
    _add_generate_summary_parser_arguments(p)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description="Process a directory of ERA5 NetCDF files containing total "
                    "precipitation (tp) and 2-meter temperature (t2m) data into "
                    "summary files."
    )
    _add_generate_summary_parser_arguments(parser)
    args = parser.parse_args()
    args.func(args)
