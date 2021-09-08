# ERA 5 Ingest Utilities

This directory contains tools that are helpful for obtaining ERA 5 data and processing it for use in the Global Water Security Center apps and analyses. Each module contains functions that can imported and scripted into custom workflows. Each module also contains a commandline interface allowing it to be called as a standalone script.

## Utility Scripts

The following utility scripts are included:

### download.py

Downloads multiple 24 hour reanalysis-era5-single-levels (ran-sfc) datasets, one for each day in the given date range, containing total precipitation (tp) and 2-meter temperature (t2m) data. Automatically downloads data from the 1979-2021 or 1950-1978 datasets based on date range. You must acquire an CDS API key AND accept the terms to download data(See **Prerequisites** section of: [How to download ERA5](https://confluence.ecmwf.int/display/CKB/How+to+download+ERA5)).

Example usage:

```bash
# Download 356 days of data data (2020-01-01 to 2021-01-01), up to 16 at a time
python download.py 2021-01-01 365 era5_pnt_hourly_2020 -k myCDSapiKEY -p 16
```

### verify.py

Checks for missing files after a bulk download. Can be instructed to download missing files.

Example usage:

```bash
# Check for missing files and download (up to 16 at a time)
python verify.py era5_pnt_hourly_2020 -d -p 16
```

### generate_summary_files.py

Process a directory of ERA5 NetCDF files containing hourly total precipitation (tp) and 2-meter temperature (t2m) data (24-hours in each file) into summary files.

Example usage:

```bash
# Process a directory of files, 24 at a time
python generate_daily_dataset.py era5_pnt_hourly_2020 era5_pnt_daily_2020 -p 24
```

### netcdf_to_zarr.py

Convert a directory of ERA5 NetCDF daily summary files into a single zarr dataset. Each file should contain a single day of data for the following variables: min_t2m_c, mean_t2m_c, max_t2m_c, and sum_tp_mm.

Example usage:

```bash
# Convert a directory of daily netcdf files into a zarr dataset with ~150 MB chunks
python netcdf_to_zarr.py era5_pnt_daily_2010_2020 era5_pnt_daily_2010_2020.zarr -s 134200000
```

### rechunk_for_time.py

Rechunk a dataset such that all time steps for a given location in the grid are contained in a single chunk to allow for more efficient time-series analysis. This script uses the [Rechunker](https://rechunker.readthedocs.io/en/latest/) library (see: [Rechunker: The missing link for chunked array analytics](https://medium.com/pangeo/rechunker-the-missing-link-for-chunked-array-analytics-5b2359e9dc11) for more details).

```bash
python rechunk_for_time.py era5_pnt_daily_2010_2020.zarr era5_pnt_daily_2010_2020_time_chunks.zarr temp.zarr -m 500MB
```

### generate_normals_dataset.py

Compute the normal (day-of-year (DOY) mean) for given variables in the provided Zarr dataset. Creates one xarray Dataset for each DOY, with dimensions "time", "latitude", and "longitude" and coordinates "time", "latitude", "longitude", "doy" with "doy" being a secondary coordinate for the "time" dimension. The "time" dimension is populated with an arbitrary datetime datetime from the year 2000 associated with the DOY. This makes the dataset easier to work with in systems that expect datetimes for a time-related dimension (e.g. THREDDS).

```bash
# Create normal (day-of-year mean) datasets (one for each day of the year) for all data contained in the zarr
python generate_normals_dataset.py era5_pnt_daily_2010_2020_time_chunks.zarr era5_normal_pnt_2010_2020  -v mean_t2m_c sum_tp_mm
```


## Workflows

The scripts can be used for the following data ingest workflows.

### Daily Ingest Workflow

1. download.py
1. verify.py
1. generate_summary_files.py
1. Add new daily summary file(s) to THREDDS

### Compute Normal Datasets

1. download.py
1. verify.py
1. generate_summary_files.py
1. netcdf_to_zarr.py
1. rechunk_for_time.py
1. generate_normals_dataset.py
1. Replace normals dataset with new one in THREDDS