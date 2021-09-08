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
    Rechunk a dataset such that all time steps for a given location in the grid are contained in a
       single chunk to allow for more efficient time-series analysis.

    Args:
        in_zarr (str): Path or address to a Zarr dataset containing time-series gridded data with dimensions
            "time", "latitude", and "longitude".
        out_zarr (str): Path to address where output Zarr dataset will be written.
        temp_zarr (str): Path to address where a temporary intermediate Zarr dataset will be written.
        max_memory (str): Maximum in-memory size of chunk. Defaults to "100MB".
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


def _rechunk_for_time_command(args):
    log_level = logging.DEBUG if args.debug else logging.INFO
    setup_basic_logging(log_level)
    log.debug(f'Given arguments: {args}')
    rechunk_for_time(args.in_zarr, args.out_zarr, args.temp_zarr, args.max_memory)


def _add_rechunk_for_time_arguments(parser):
    parser.add_argument("in_zarr",
                        help='Path or address to a Zarr dataset containing time-series gridded data with dimensions '
                             '"time", "latitude", and "longitude".')
    parser.add_argument("out_zarr",
                        help="Path to address where output Zarr dataset will be written.")
    parser.add_argument("temp_zarr",
                        help="Path to address where a temporary intermediate Zarr dataset will be written.")
    parser.add_argument("-m", "--max_memory", type=str, default="100MB",
                        help='Maximum in-memory size of chunk. Defaults to "100MB".')
    parser.add_argument("-d" "--debug", dest="debug", action='store_true',
                        help="Turn on debug logging.")
    parser.set_defaults(func=_rechunk_for_time_command)


def _add_rechunk_for_time_parser(subparsers):
    p = subparsers.add_parser(
        'era5-rechunk-for-time',
        description='Rechunk a dataset such that all time steps for a given location in the grid are contained in a '
                    'single chunk to allow for more efficient time-series analysis.'
    )
    _add_rechunk_for_time_arguments(p)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Rechunk a dataset such that all time steps for a given location in the grid are contained in a '
                    'single chunk to allow for more efficient time-series analysis.'
    )
    _add_rechunk_for_time_arguments(parser)
    args = parser.parse_args()
    args.func(args)

