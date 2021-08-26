import datetime as dt
import logging
from pathlib import Path
from pprint import pformat
import shutil

import humanize
from rechunker import rechunk
import xarray as xr

from gwsc_ingest.utils.logging import setup_basic_logging

log = logging.getLogger(__name__)


def rechunk_for_time(in_zarr, out_zarr, temp_zarr, max_memory):
    """

    Args:
        in_zarr:
        out_zarr:
        temp_zarr:

    Returns:

    """
    # Clean up
    out_zarr_path = Path(out_zarr)
    if out_zarr_path.is_dir():
        shutil.rmtree(out_zarr_path)
    temp_zarr_path = Path(temp_zarr)
    if temp_zarr_path.is_dir():
        shutil.rmtree(temp_zarr_path)

    with xr.open_zarr(in_zarr) as ds:
        log.debug(ds)
        log.debug(f'Dimensions: {ds.dims}')

        # TODO: Compute lat/lon chunk size

        chunks_per_var = {
            'time': ds.time.size,
            'latitude': 7,  # Factors of 721 = 1 x 7 x 103
            'longitude': 180,  # Factors of 1440 = 1 x 2^5 x 3^2 x 5
        }

        target_chunks = dict()
        for var in ds.variables:
            if var not in chunks_per_var:
                target_chunks[var] = chunks_per_var
            else:
                target_chunks[var] = None

        log.debug(f'Target Chunks:\n{pformat(target_chunks)}')

        target_chunks_size = dict()
        for var in target_chunks:
            var_data = target_chunks[var]
            if var_data is not None:
                items_per_chunk = var_data['time'] * var_data['latitude'] * var_data['longitude']
                var_item_size = ds[var].data.itemsize
                target_chunks_size[var] = {
                    'chunk_size': humanize.naturalsize(items_per_chunk * var_item_size),
                    'item_size': humanize.naturalsize(var_item_size),
                    'items_per_chunk': items_per_chunk,
                }
            else:
                target_chunks_size[var] = None

        log.debug(f'Target Chunks Size:\n{pformat(target_chunks_size)}')

        array_plan = rechunk(
            source=ds,
            target_chunks=target_chunks,
            max_mem=max_memory,
            target_store=out_zarr,
            temp_store=temp_zarr,
        )

        log.info('Executing...')
        start_time = dt.datetime.utcnow()
        array_plan.execute()
        compute_time = dt.datetime.utcnow() - start_time
        log.debug(f'Done. Execution took: {humanize.naturaldelta(compute_time)}')


if __name__ == '__main__':
    setup_basic_logging(logging.DEBUG)
    # in_zarr = r'E:\ERA5\era5_pnt_daily_1950_2021.zarr'
    # out_zarr = r'E:\ERA5\era5_pnt_daily_1950_2021_by_time.zarr'
    # temp_zarr = r'E:\ERA5\era5_pnt_daily_1950_2021_by_time-temp.zarr'
    # max_memory = '500MB'

    rechunk_for_time(
        in_zarr=in_zarr,
        out_zarr=out_zarr,
        temp_zarr=temp_zarr,
        max_memory=max_memory,
    )
