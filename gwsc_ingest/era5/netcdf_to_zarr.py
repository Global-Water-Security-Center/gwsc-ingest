import logging
import math

import humanize
from tqdm import trange
import xarray as xr

from gwsc_ingest.utils.logging import setup_basic_logging
from gwsc_ingest.utils.validation import validate_directory

log = logging.getLogger(__name__)


def netcdf_to_zarr(in_directory, out_zarr, size_per_chunk=1.049e8):
    """
    Process a directory of ERA5 NetCDF daily summary files, each containing
        a single day of data for the following variables: min_t2m_c, mean_t2m_c,
        max_t2m_c, and sum_tp_mm.

    Args:
        in_directory (str): Path to directory containing ERA5 NetCDF daily summary files.
        out_zarr (str): Path to zarr directory where zarr data will be written to.
        size_per_chunk (int): Target size per chunk in bytes. Defaults to 100 MB (1.049e8).
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


def _netcdf_to_zarr_command(args):
    log_level = logging.DEBUG if args.debug else logging.INFO
    setup_basic_logging(log_level)
    log.debug(f'Given arguments: {args}')
    netcdf_to_zarr(
        in_directory=args.in_directory,
        out_zarr=args.out_zarr,
        size_per_chunk=args.size_per_chunk
    )


def _add_netcdf_to_zarr_arguments(parser):
    parser.add_argument("in_directory",
                        help="Path to directory containing ERA5 NetCDF daily summary files "
                             "(e.g.: '/data/pnt_daily_1950_2021_w_time').")
    parser.add_argument("out_zarr",
                        help="Path to zarr directory where zarr data will be written to "
                             "(e.g.: '/data/pnt_daily_1950_2021.zarr').")
    parser.add_argument("-s" "--size_per_chunk", dest="size_per_chunk", type=int, required=False, default=1.049e8,
                        help="Target size per chunk in bytes. Defaults to 100 MB (1.049e8).")
    parser.add_argument("-d" "--debug", dest="debug", action='store_true',
                        help="Turn on debug logging.")
    parser.set_defaults(func=_netcdf_to_zarr_command)


def _add_netcdf_to_zarr_parser(subparsers):
    p = subparsers.add_parser(
        'era5-netcdf-to-zarr',
        description="Convert a directory of ERA5 NetCDF daily summary files into a "
                    "single zarr dataset. Each file should contain a single day of "
                    "data for the following variables: min_t2m_c, mean_t2m_c, "
                    "max_t2m_c, and sum_tp_mm."
    )
    _add_netcdf_to_zarr_arguments(p)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description="Convert a directory of ERA5 NetCDF daily summary files into a "
                    "single zarr dataset. Each file should contain a single day of "
                    "data for the following variables: min_t2m_c, mean_t2m_c, "
                    "max_t2m_c, and sum_tp_mm."
    )

    _add_netcdf_to_zarr_arguments(parser)
    args = parser.parse_args()
    args.func(args)
