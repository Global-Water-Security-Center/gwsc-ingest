from .download import _add_download_parser
from .verify import _add_verify_parser
from .generate_summary_files import _add_generate_summary_parser
from .netcdf_to_zarr import _add_netcdf_to_zarr_parser
from .rechunk_for_time import _add_rechunk_for_time_parser
from .generate_normals_dataset import _add_generate_normal_parser


def add_era5_parsers(subparsers):
    _add_download_parser(subparsers)
    _add_verify_parser(subparsers)
    _add_generate_summary_parser(subparsers)
    _add_netcdf_to_zarr_parser(subparsers)
    _add_rechunk_for_time_parser(subparsers)
    _add_generate_normal_parser(subparsers)
