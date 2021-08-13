import datetime as dt
import logging
import multiprocessing as mp
from pathlib import Path

import humanize
import numpy as np
from tqdm import tqdm
import xarray as xr

from gwsc_ingest.utils.logging import setup_basic_logging
from gwsc_ingest.utils.validation import validate_directory

log = logging.getLogger(__name__)


def bulk_add_time_to_summary_file(in_directory, out_directory, processes=1):
    """
    Add times dimension from file name.

    Args:
        in_directory:
        out_directory:
        processes:
    """
    in_directory = validate_directory(in_directory)
    out_directory = validate_directory(out_directory)

    # Save current time for timing
    start_time = dt.datetime.utcnow()

    # Build task arguments
    tasks = []
    for item in in_directory.iterdir():
        if item.is_file() and 'nc' in item.suffix:
            tasks.append({
                'in_filename': str(item),
                'out_filename': str(out_directory / item.name)
            })

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


# Define worker function wrapper
def _worker(qwargs):
    add_time_to_netcdf_file(
        in_filename=qwargs.get('in_filename'),
        out_filename=qwargs.get('out_filename'),
    )


def add_time_to_netcdf_file(in_filename, out_filename, overwrite=False):
    """
    Adds the time dimension to the netcdf files produced by initial version of generate_summary_files.

    Args:
        in_filename (str): Path to netcdf file without time dimension.
        out_filename (str): Path to netcdf file that will be written w/ time dimension.
        overwrite (bool): Overwrite out_filename if it exists when True. Defaults to False.
    """
    try:
        if Path(out_filename).is_file() and not overwrite:
            log.warning(f'The file "{out_filename}" already exists. Skipping...')
            return

        with xr.load_dataset(in_filename) as ds:
            da_s = {}
            lats = ds.latitude.data.copy()
            lons = ds.longitude.data.copy()
            filename_datetime = dt.datetime.strptime(Path(in_filename).stem, 'reanalysis-era5-sfc-daily-%Y-%m-%d')
            times = np.array([filename_datetime])

            for var in ds.variables:
                if var in ['longitude', 'latitude']:
                    continue
                da_s.update({
                    var: xr.DataArray(
                        np.array([ds[var].data.copy()]),
                        coords=[("time", times), ('latitude', lats), ('longitude', lons)],
                        attrs=ds[var].attrs
                    )
                })

            new_ds = xr.Dataset(da_s)

        new_ds.to_netcdf(out_filename)
    except Exception:
        log.exception('An unexpected error occurred:')


if __name__ == '__main__':
    in_dir = Path(r'E:\ERA5\pnt_daily_1950_2021')
    out_dir = r'E:\ERA5\pnt_daily_1950_2021_w_time'
    setup_basic_logging(logging.DEBUG)
    bulk_add_time_to_summary_file(in_dir, out_dir, processes=48)
