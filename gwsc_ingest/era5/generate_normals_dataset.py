import copy
import datetime as dt
import logging
import traceback

import humanize
import numpy as np
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

        # Group data by day-of-year
        doy_groups = template_da.groupby("time.dayofyear").groups

        # Track failed doy mean computations
        failed = {v: [] for v in variables}
        for variable in variables:
            first_year = ds[variable]["time"][0].dt.year.item()
            last_year = ds[variable]["time"][-1].dt.year.item()
            for doy, doy_indices in tqdm(doy_groups.items()):
                # Check for existing output for this doy
                out_nc_file = out_directory / f'{variable}_doy_mean_{doy}_{first_year}_{last_year}.nc'
                if out_nc_file.is_file():
                    if not overwrite:
                        log.info(f'\nOutput for doy {doy} found at: {out_nc_file}. Skipping...')
                        continue
                    else:
                        out_nc_file.unlink(missing_ok=True)

                # Start computation for current DOY
                log.info(f'\nComputing DOY Mean for DOY {doy} for variable {variable}...')
                comp_start_time = dt.datetime.utcnow()

                # Compute mean for the current doy for all variables in parallel
                result = _compute_doy_mean(variable, ds[variable], doy, doy_indices)

                if not result['success']:
                    log.error(f'An error occurred while processing mean for {variable} for DOY {doy}:\n'
                              f'{result["exception"]}\n'
                              f'{result["traceback"]}')
                    failed[variable].append(doy)
                    continue

                result_da = result['result']
                log.info(f'DOY Mean Computation for {variable} for DOY {doy} took '
                         f'{humanize.naturaldelta(dt.datetime.utcnow() - comp_start_time)}')

                # Create dataset for writing
                out_ds = xr.Dataset(
                    data_vars={result_da.attrs['long_name']: result_da},
                    coords={
                        'doy': np.array([doy]),
                        'latitude': lats,
                        'longitude': lons,
                    },
                )
                out_ds = out_ds.chunk(chunks={'doy': 1, 'latitude': len(lats), 'longitude': len(lons)})
                log.info(f'Out DataSet:\n'
                         f'{out_ds}')

                log.info(f'Writing output: {out_nc_file}')
                out_ds.to_netcdf(out_nc_file)
                log.info(f'Processing complete for {variable} for DOY {doy}.')

        # Log summary of failed processing
        for variable, failed_doys in failed.items():
            if not failed_doys:
                continue
            log.warning(f'Processing failed for the following DOYs for {variable}: '
                        f'{" ".join(failed_doys)}')


def _compute_doy_mean(variable, da, doy, doy_indices):
    """

    Args:
        variable (str):
        da (xr.DataArray):
        doy (int):
        doy_indices (dict): ?

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
        doys = np.array([doy])
        lats = da.latitude.data
        lons = da.longitude.data
        coords = [('doy', doys), ('latitude', lats), ('longitude', lons)]

        # Attrs
        doy_mean_name = variable + '_doy_mean'
        attrs = copy.deepcopy(da.attrs)
        attrs['long_name'] = doy_mean_name

        mean_da_doy = xr.DataArray(
            np.array([mean_da.data]),
            coords=coords,
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
    # in_zarr = r'E:\ERA5\era5_pnt_daily_1950_2021.zarr'
    out_directory = r'E:\ERA5\era5_pnt_doy_mean_1950_2021'
    # vars = ['mean_t2m_c', 'sum_tp_mm']
    variables = ['mean_t2m_c']
    setup_basic_logging(logging.INFO)
    generate_normals_dataset(
        in_zarr=in_zarr,
        out_directory=out_directory,
        variables=variables
    )
