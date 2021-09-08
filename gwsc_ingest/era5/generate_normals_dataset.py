import copy
import datetime as dt
import logging
import traceback

import humanize
import numpy as np
import pandas as pd
from tqdm import tqdm
import xarray as xr

from gwsc_ingest.utils.logging import setup_basic_logging
from gwsc_ingest.utils.validation import validate_directory


log = logging.getLogger(__name__)


def generate_normals_dataset(in_zarr, out_directory, variables=None, overwrite=False):
    """
    Compute the normal (day-of-year (DOY) mean) for given variables in the provided Zarr dataset. Creates one
        xarray Dataset for each DOY, with dimensions "time", "latitude", and "longitude" and coordinates "time",
        "latitude", "longitude", "doy" with "doy" being a secondary coordinate for the "time" dimension. The "time"
        dimension is populated with an arbitrary datetime datetime from the year 2000 associated with the DOY. This
        makes the dataset easier to work with in systems that expect datetimes for a time-related dimension
        (e.g. THREDDS).

    Args:
        in_zarr (str): Path or address to a Zarr dataset containing time-series gridded data with dimensions
            "time", "latitude", and "longitude".
        out_directory (str): Path to directory where output will be written.
        variables (iterable): A list/tuple of variable names on which to compute day-of-year means. If not provided,
            all variables that are not dimension or coordinate variables will be processed.
        overwrite (bool): Overwrite existing output files if True. Defaults to False, skipping files/DOYs
            that already exist.

    """
    out_directory = validate_directory(out_directory)
    log.info(f'Results will be written to {out_directory}')

    with xr.open_zarr(in_zarr) as ds:
        log.debug(f'Given Dataset:\n{ds}')

        # Use all variable if not provided
        if not variables:
            variables = [v for v in ds.variables if v not in ds.dims and v not in ds.coords]

        log.info(f'Computing day-of-year mean on the following variables: {" & ".join(variables)}')

        # Use first variable as template DataArray
        template_da = ds[variables[0]]
        lats = template_da.latitude.data.copy()
        lons = template_da.longitude.data.copy()

        # Create lookup array of dates to assign to each doy
        # THREDDS needs dates, so year 2000 chosen as an arbitrary leap year
        # Prepend extra day to beginning so lookup can be 1-indexed instead of zero-indexed
        datetime_for_ = pd.date_range(
            start=dt.datetime(year=1999, month=12, day=31),
            end=dt.datetime(year=2000, month=12, day=31),
            freq='D'
        ).to_list()

        # Track failed doy mean computations
        failed = {v: [] for v in variables}
        ref_period_start = ds["time"][0].dt.strftime('%Y-%m-%d').item()
        ref_period_end = ds["time"][-1].dt.strftime('%Y-%m-%d').item()

        # Group data by day-of-year
        doy_groups = template_da.groupby("time.dayofyear").groups
        doy_group_indices = [(d, i) for d, i in doy_groups.items()]

        for doy, doy_indices in tqdm(doy_group_indices):
            # Get arbitrary date for given day-of-year
            doy_date = datetime_for_[doy]

            # Build up data_vars arg for dataset
            data_vars = dict()

            # Determine output file format
            out_file = out_directory / \
                       f'reanalysis-era5-normal-pnt-{doy_date:%Y-%m-%d}.nc'

            if out_file.is_file():
                if not overwrite:
                    log.info(f'\nOutput for doy {doy} found at: {out_file}. Skipping...')
                    continue
                else:
                    out_file.unlink(missing_ok=True)

            for variable in variables:
                # Start computation for current DOY
                log.info(f'\nComputing mean for DOY {doy} for variable {variable}...')
                comp_start_time = dt.datetime.utcnow()

                # Compute mean for the current doy for all variables in parallel
                result = _compute_doy_mean(variable, ds[variable], doy, doy_indices, doy_date,
                                           ref_period_start, ref_period_end)

                if result['success'] is None:
                    log.info(f'An unexpected error occurred while processing {variable} for DOY {doy}')
                    failed[variable].append(str(doy))
                    continue
                if result['success'] is False:
                    log.error(f'An error occurred while processing mean for {variable} for DOY {doy}:\n'
                              f'{result["result"]["exception"]}\n'
                              f'{result["result"]["traceback"]}')
                    failed[variable].append(str(doy))
                    continue

                result_da = result['result']
                log.info(f'Mean computation for DOY {doy} for {variable} took '
                         f'{humanize.naturaldelta(dt.datetime.utcnow() - comp_start_time)}')

                data_vars.update({result_da.attrs['long_name']: result_da})

            # Create dataset for writing - write one file for each DOY
            out_ds = xr.Dataset(
                data_vars=data_vars,
                attrs={
                    'reference_period_start': ref_period_start,
                    'reference_period_end': ref_period_end,
                }
            )
            out_ds = out_ds.chunk(chunks={'time': 1, 'latitude': len(lats), 'longitude': len(lons)})
            log.info(f'Out DataSet:\n'
                     f'{out_ds}')

            log.info(f'Writing output: {out_file}')
            out_ds.to_netcdf(out_file)
            log.info(f'Processing complete for {variable} for DOY {doy}.')

        # Log summary of failed processing
        has_failures = False
        for variable, failed_doys in failed.items():
            if not failed_doys:
                continue
            has_failures = True
            log.warning(f'Processing failed for the following DOYs for {variable}: '
                        f'{" ".join(failed_doys)}')

        if has_failures:
            log.warning(f'Process completed with failures. Please re-run to correct failures and continue.')
            exit(1)


def _compute_doy_mean(variable, da, doy, doy_indices, doy_date, ref_start, ref_end):
    """
    Compute the DOY mean on the given data array, using the given doy_indices to retrieve the values for the
        given doy and complete the computation. This was originally split into a separate function so that the process
        could be multi-processed. However, the calculation appears to be disk bound, meaning multi-processing won't
        improve performance unless the input Zarr dataset is stored on a distributed data storage system.

    Args:
        variable (str): Name of the variable. Used to create the name of the output DataArray.
        da (xr.DataArray): The xr.DataArray containing the dataset on which to compute the DOY mean.
        doy (int):  Day of year to compute the mean.
        doy_indices (list): Time indices of all times corresponding with the DOY.
        doy_date (np.datetime): Datetime corresponding with the DOY.
        ref_start (str): Datetime string of the start of the reference period.
        ref_end (str): Datetime string of the end of the reference period.

    Returns:
        xr.DataArray or None:
    """
    result = {
        'success': None,
        'result': None
    }

    try:
        # Trigger mean computation
        group_da = da.isel({'time': doy_indices})
        mean_da = group_da.mean('time').compute()

        # Coords
        times = np.array([doy_date])
        doys = np.array([np.int16(doy)])
        lats = da.latitude.data
        lons = da.longitude.data

        # Attrs
        doy_mean_name = 'normal_' + variable
        attrs = copy.deepcopy(da.attrs)
        attrs['long_name'] = doy_mean_name
        attrs['reference_period_start'] = ref_start
        attrs['reference_period_end'] = ref_end

        mean_da_doy = xr.DataArray(
            np.array([mean_da.data]),
            coords={
                'time': times,
                'latitude': lats,
                'longitude': lons,
                'doy': ('time', doys),
            },
            dims=['time', 'latitude', 'longitude'],
            attrs=attrs
        )

        result['success'] = True
        result['result'] = mean_da_doy

    except KeyboardInterrupt:
        exit(1)

    except Exception as e:
        result['success'] = False
        result['result'] = {
            'exception': str(e),
            'traceback': traceback.format_exc()
        }

    return result


def _generate_normal_command(args):
    log_level = logging.DEBUG if args.debug else logging.INFO
    setup_basic_logging(log_level)
    log.debug(f'Given arguments: {args}')
    generate_normals_dataset(args.in_zarr, args.out_directory, args.variables, args.overwrite)


def _add_generate_normal_arguments(parser):
    parser.add_argument("in_zarr",
                        help='Path or address to a Zarr dataset containing time-series gridded data with dimensions '
                             '"time", "latitude", and "longitude"')
    parser.add_argument("out_directory",
                        help="Path to directory where output netcdf files will be written.")
    parser.add_argument("-v", "--variables", nargs='+', default=None,
                        help="One or more variable names on which to compute day-of-year means. If not provided, "
                             "all variables that are not dimension or coordinate variables will be processed.")
    parser.add_argument("-o", "--overwrite", action="store_true", default=False,
                        help="Overwrite existing output files if True. Defaults to False, skipping files/DOYs "
                             "that already exist.")
    parser.add_argument("-d" "--debug", dest="debug", action='store_true',
                        help="Turn on debug logging.")
    parser.set_defaults(func=_generate_normal_command)


def _add_generate_normal_parser(subparsers):
    p = subparsers.add_parser(
        'era5-gen-normal-ds',
        description='Compute the normal (day-of-year (DOY) mean) for given variables in the provided Zarr dataset. '
                    'Creates one xarray Dataset for each DOY, with dimensions "time", "latitude", and "longitude" and '
                    'coordinates "time", "latitude", "longitude", "doy" where "doy" is a secondary coordinate for '
                    'the "time" dimension. The "time" dimension is populated with an arbitrary datetime from '
                    'the year 2000 associated with the DOY. This makes the dataset easier to work with in systems that '
                    'expect datetimes for a time-related dimension (e.g. THREDDS).'
    )
    _add_generate_normal_arguments(p)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Compute the normal (day-of-year (DOY) mean) for given variables in the provided Zarr dataset. '
                    'Creates one xarray Dataset for each DOY, with dimensions "time", "latitude", and "longitude" and '
                    'coordinates "time", "latitude", "longitude", "doy" where "doy" is a secondary coordinate for '
                    'the "time" dimension. The "time" dimension is populated with an arbitrary datetime from '
                    'the year 2000 associated with the DOY. This makes the dataset easier to work with in systems that '
                    'expect datetimes for a time-related dimension (e.g. THREDDS).'
    )
    _add_generate_normal_arguments(parser)
    args = parser.parse_args()
    args.func(args)
