import logging
import math
from pathlib import Path

import humanize
from tqdm import trange
import xarray as xr

from gwsc_ingest.utils.logging import setup_basic_logging
from gwsc_ingest.utils.validation import validate_directory

log = logging.getLogger(__name__)


def netcdf_to_zarr(in_directory, out_zarr, size_per_chunk=1.049e8):
    """
    Combine many netcdf files into one xarray-style zarr dataset.

    Args:
        in_directory:
        out_zarr:
        size_per_chunk (int): target size per chunk. Defaults to 100 MB (1.049e8).
    """
    in_directory = validate_directory(in_directory)
    log.info('Identifying files...')
    nc_files = sorted([item for item in in_directory.iterdir() if item.is_file() and 'nc' in item.suffix])
    num_files = len(nc_files)
    log.info(f'Found {num_files} files.')

    # Compute chunk sizes using first files as a template
    template_dataset = nc_files[0]
    log.info(f'Using "{template_dataset}" as the template dataset:')
    with xr.load_dataset(template_dataset) as ds:
        log.debug(ds)

        # Estimate size of dset
        size_per_ds = 0
        for var in ds.variables:
            var_size = ds[var].size * ds[var].data.itemsize
            log.debug(f'{var}: {humanize.naturalsize(var_size)}')

            # Skip dimension variables for ds size estimate
            if var in ds.dims:
                continue

            size_per_ds += var_size

        ds_per_chunk = math.ceil(size_per_chunk / size_per_ds)
        num_chunks = math.ceil(num_files / ds_per_chunk)
        log.info(f'Size per dataset: {humanize.naturalsize(size_per_ds)}')
        log.info(f'Chunk datasets: {ds_per_chunk} datasets')
        log.info(f'Chunk size: {humanize.naturalsize(ds_per_chunk * size_per_ds)}')
        log.info(f'Num chunks: {num_chunks}')

    for i in trange(num_chunks):
        fstart = i * ds_per_chunk
        fstop = (i + 1) * ds_per_chunk
        fstop = int(min(fstop, num_files))

        combined_ds = xr.open_mfdataset(
            nc_files[fstart:fstop],
            combine='by_coords',
            concat_dim='time',
        )

        # Re-chunk by time
        combined_ds = combined_ds.chunk(chunks={'time': ds_per_chunk})
        log.debug(combined_ds)

        if i == 0:
            combined_ds.to_zarr(out_zarr, consolidated=True, mode='w')
        else:
            combined_ds.to_zarr(out_zarr, consolidated=True, append_dim='time')


if __name__ == '__main__':
    in_dir = Path(r'E:\ERA5\pnt_daily_1950_2021_w_time')
    out_zarr = r'E:\ERA5\testing\era5_pnt_daily_1950_2021.zarr'
    setup_basic_logging(logging.INFO)
    netcdf_to_zarr(in_dir, out_zarr)
