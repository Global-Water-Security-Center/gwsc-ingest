import copy
import datetime as dt
import logging
import multiprocessing as mp
from pathlib import Path

import humanize
import numpy as np
from tqdm import tqdm
import xarray as xr

from gwsc_ingest.utils.logging import setup_basic_logging


log = logging.getLogger(__name__)


def _compute_doy_mean(qwargs):
    """

    Args:
        variable:
        da:
        doy:
        doy_indices:

    Returns:

    """
    variable = qwargs.get('variable')
    da = qwargs.get('da')
    doy = qwargs.get('doy')
    doy_indices = qwargs.get('doy_indices')
    comp_start_time = dt.datetime.utcnow()

    # Trigger mean computation
    group_da = da.isel({'time': doy_indices})
    mean_da = group_da.mean('time').compute()

    log.debug(f'Computation for var {variable} doy {doy} took: '
              f'{humanize.naturaldelta(dt.datetime.utcnow() - comp_start_time)}')

    # Coords
    doys = np.array([doy])
    lats = da.latitude.data
    lons = da.longitude.data
    coords = [('doy', doys), ('latitude', lats), ('longitude', lons)]

    # Attrs
    mean_variable_name = 'doy_mean_' + variable
    attrs = copy.deepcopy(da.attrs)
    attrs['long_name'] = mean_variable_name
    log.debug(f'Attributes for {variable}: {attrs}')

    mean_da_doy = xr.DataArray(
        np.array([mean_da.data]),
        coords=coords,
        attrs=attrs
    )

    return mean_da_doy


def generate_normals_dataset(in_zarr, out_netcdf, variables=None):
    """
    Compute the normal precipitation and temperatures datasets.
    Args:
        in_zarr:
        out_netcdf:

    Returns:

    """
    with xr.open_zarr(in_zarr) as ds:
        p_in_zarr = Path(in_zarr)
        working_dir = p_in_zarr.parent
        out_zarr = working_dir / f'{p_in_zarr.stem}_doy_mean.zarr'
        log.info(f'Results will be written to {out_zarr}')
        log.debug(f'Given Dataset:\n{ds}')

        if not variables:
            variables = [v for v in ds.variables if v not in ds.dims]
        log.info(f'Computing day-of-year mean on the following variables: {" & ".join(variables)}')

        # Use first variable as template DataArray
        template_da = ds[variables[0]]
        lats = template_da.latitude.data.copy()
        lons = template_da.longitude.data.copy()
        doy_groups = template_da.groupby("time.dayofyear").groups

        for doy, doy_indices in tqdm(doy_groups.items()):
            with mp.Pool(processes=len(variables)) as pool:
                tasks = []
                for variable in variables:
                    tasks.append({
                        'variable': variable,
                        'da': ds[variable],
                        'doy': doy,
                        'doy_indices': doy_indices,
                    })

                comp_start_time = dt.datetime.utcnow()
                # Compute mean for the current doy for all variables in parallel
                data_vars = dict()
                for r in pool.imap_unordered(_compute_doy_mean, tasks):
                    data_vars.update({r.attrs['long_name']: r})

                log.info(f'Computation for doy {doy} took '
                         f'{humanize.naturaldelta(dt.datetime.utcnow() - comp_start_time)}')

                # Prepare a dataset for writing
                out_ds = xr.Dataset(
                    data_vars=data_vars,
                    coords={
                        'doy': np.array([doy]),
                        'latitude': lats,
                        'longitude': lons,
                    },
                )
                out_ds.chunk(chunks={'doy': 1, 'latitude': len(lats), 'longitude': len(lons)})

                if out_zarr.is_dir():
                    out_ds.to_zarr(out_zarr, mode='a', append_dim='doy', consolidated=True)
                else:
                    out_ds.to_zarr(out_zarr, mode='w', consolidated=True)

    log.info(f'Writing day-of-year dataset to single netcdf: {out_ds}')
    netcdf_write_start = dt.datetime.utcnow()
    with xr.open_zarr(out_zarr) as out_zarr_ds:
        log.debug(f'Out Zarr DS:\n{out_zarr_ds}')
        out_zarr_ds.to_netcdf(out_netcdf)
    log.info(f'Writing day-of-year dataset to netcdf took '
             f'{humanize.naturaldelta(dt.datetime.utcnow() - netcdf_write_start)}')


if __name__ == '__main__':
    in_zarr_path = r'E:\ERA5\era5_pnt_daily_1950_2021_by_time.zarr'
    # in_zarr_path = r'E:\ERA5\era5_pnt_daily_1950_2021.zarr'
    out_ds_path = r'E:\ERA5\era5_t_doy_mean_1950_2021.nc'
    # vars = ['mean_t2m_c', 'sum_tp_mm']
    vars = ['mean_t2m_c']
    setup_basic_logging(logging.DEBUG)
    generate_normals_dataset(in_zarr_path, out_ds_path, vars)
