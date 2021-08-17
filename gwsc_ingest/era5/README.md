# ERA 5 Ingest Utilities

This directory contains tools that are helpful for obtaining ERA 5 data and processing it for use in the Global Water Security Center apps and analyses.

## Utility Scripts

The following utility scripts are included:

### download.py

Downloads multiple 24 hour reanalysis-era5-single-levels (ran-sfc) datasets, one for each day in the given date range, containing total precipitation (tp) and 2-meter temperature (t2m) data. Automatically downloads data from the 1979-2021 or 1950-1978 datasets based on date range. You must acquire an CDS API key AND accept the terms to download data(See **Prerequisites** section of: [How to download ERA5](https://confluence.ecmwf.int/display/CKB/How+to+download+ERA5)).

Example usage:

```bash
# Download 356 days of data data (2020-01-01 to 2021-01-01), up to 16 at a time
python download.py 2021-01-01 365 /data/era5_pnt_hourly_2020 -k myCDSapiKEY -p 16
```

### verify.py

Checks for missing files after a bulk download. Can be instructed to download missing files.

Example usage:

```bash
# Check for missing files and download (up to 16 at a time)
python verify.py /data/era5_pnt_hourly_2020 -d -p 16
```

### generate_summary_files.py

Process a directory of ERA5 NetCDF files containing hourly total precipitation (tp) and 2-meter temperature (t2m) data (24-hours in each file) into summary files.

Example usage:

```bash
# Process a directory of files, 24 at a time
python generate_normals_dataset.py /data/era5_pnt_hourly_2020 /data/era5_pnt_daily_2020 -p 24
```

### netcdf_to_zarr.py

Convert a directory of ERA5 NetCDF daily summary files into a single zarr dataset. Each file should contain a single day of data for the following variables: min_t2m_c, mean_t2m_c, max_t2m_c, and sum_tp_mm.

Example usage:

```bash
# Convert a directory of daily netcdf files into a zarr dataset with ~128 MB chunks
python netcdf_to_zarr.py /data/era5_pnt_daily_2020 /data/era5_pnt_daily_2020.zarr -s 1.342e8
```

### rechunk_for_time.py

### generate_normals_dataset.py


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