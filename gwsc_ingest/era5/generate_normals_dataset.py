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
    Compute the day-of-year mean (DOY) / normal for given variables in the provided Zarr dataset.

    Args:
        in_zarr:
        out_directory:
        variables:
        overwrite:

    """
    out_directory = validate_directory(out_directory)
    log.info(f'Results will be written to {out_directory}')

    with xr.open_zarr(in_zarr) as ds:
        log.debug(f'Given Dataset:\n{ds}')

        # Use all variable if not provided
        if not variables:
            variables = [v for v in ds.variables if v not in ds.dims]
        log.info(f'Computing day-of-year mean on the following variables: {" & ".join(variables)}')

        # Use first variable as template DataArray
        template_da = ds[variables[0]]
        lats = template_da.latitude.data.copy()
        lons = template_da.longitude.data.copy()

        # Create lookup array of dates to assign to each doy
        # THREDDS needs dates, so year 2000 chosen as an arbitrary leap year
        # Prepend extra day to begining so lookup can be 1-indexed instead of zero-indexed
        datetime_for_ = pd.date_range(
            start=dt.datetime(year=1999, month=12, day=31),
            end=dt.datetime(year=2000, month=12, day=31),
            freq='D'
        ).to_list()

        # Group data by day-of-year
        doy_groups = template_da.groupby("time.dayofyear").groups

        # Track failed doy mean computations
        failed = {v: [] for v in variables}
        for variable in variables:
            for doy, doy_indices in tqdm(doy_groups.items()):
                # Convert doy to date
                doy_date = datetime_for_[doy]
                # Check for existing output for this doy
                filename_variable = 'temp' if 'temp' in variable else 'prcp'
                out_file = out_directory / f'reanalysis-era5-normal-{filename_variable}-{doy_date:%Y-%m-%d}.nc'
                out_file = out_directory / f'reanalysis-era5-normal-{variable.replace("_", "-")}-{doy_date:%Y-%m-%d}.nc'

                if out_file.is_file():
                    if not overwrite:
                        log.info(f'\nOutput for doy {doy} found at: {out_file}. Skipping...')
                        continue
                    else:
                        out_file.unlink(missing_ok=True)

                # Start computation for current DOY
                log.info(f'\nComputing DOY Mean for DOY {doy} for variable {variable}...')
                comp_start_time = dt.datetime.utcnow()

                # Compute mean for the current doy for all variables in parallel
                result = _compute_doy_mean(variable, ds[variable], doy, doy_indices, doy_date)

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
                log.info(f'DOY Mean Computation for {variable} for DOY {doy} took '
                         f'{humanize.naturaldelta(dt.datetime.utcnow() - comp_start_time)}')

                # Create dataset for writing
                out_ds = xr.Dataset(
                    data_vars={result_da.attrs['long_name']: result_da},
                )
                out_ds = out_ds.chunk(chunks={'doy': 1, 'latitude': len(lats), 'longitude': len(lons)})
                log.info(f'Out DataSet:\n'
                         f'{out_ds}')

                log.info(f'Writing output: {out_file}')
                out_ds.to_netcdf(out_file)
                log.info(f'Processing complete for {variable} for DOY {doy}.')

        # Log summary of failed processing
        for variable, failed_doys in failed.items():
            if not failed_doys:
                continue
            log.warning(f'Processing failed for the following DOYs for {variable}: '
                        f'{" ".join(failed_doys)}')


def _compute_doy_mean(variable, da, doy, doy_indices, doy_date):
    """

    Args:
        variable (str):
        da (xr.DataArray):
        doy (int):
        doy_indices (dict): ?
        doy_date :

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
        doys = np.array([doy])
        lats = da.latitude.data
        lons = da.longitude.data

        # Attrs
        doy_mean_name = variable + '_doy_mean'
        attrs = copy.deepcopy(da.attrs)
        attrs['long_name'] = doy_mean_name

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


if __name__ == '__main__':
    in_zarr = r'E:\ERA5\era5_pnt_daily_1950_2021_by_time.zarr'
    out_directory = r'E:\ERA5\era5_pnt_doy_mean_1950_2021'
    variables = ['mean_t2m_c', 'sum_tp_mm']
    setup_basic_logging(logging.INFO)
    generate_normals_dataset(
        in_zarr=in_zarr,
        out_directory=out_directory,
        variables=variables
    )
