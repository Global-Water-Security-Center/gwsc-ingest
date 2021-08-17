# ERA 5 Ingest Utilities

This directory contains tools that are helpful for obtaining ERA 5 data and processing it for use in the Global Water Security Center apps and analyses.

## Utility Scripts

The following utility scripts are included:

### add_time_dim.py

### download.py

### generate_normals_dataset.py

### generate_summary_files.py

### netcdf_to_zarr.py

### rechunk_for_time.py

### verify.py

## Workflows

### Daily Ingest Workflow

1. download.py
1. verify.py
1. generate_summary_files.py

### Compute Normal Datasets

1. download.py
1. verify.py
1. generate_summary_files.py
1. netcdf_to_zarr.py
1. rechunk_for_time.py
1. generate_normals_dataset.py