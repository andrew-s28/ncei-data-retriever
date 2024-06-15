# NCEI Data Retriever

This package provides a wrapper over the [NCEI Data Service API](https://www.ncei.noaa.gov/support/access-data-service-api-user-documentation) 
to retrieve daily weather summaries from the National Centers for Environmental Information (NCEI) database. This simplifies access to basic climate
variables and retrieves relevant metadata in a CF-compliant netCDF file.

## Installation

Assuming you have [Python](https://docs.python.org/3/) and [conda](https://conda.io/projects/conda/en/latest/index.html) installed:

1. Clone the repository and switch to the directory:
    ```
    git clone https://github.com/andrew-s28/ncei-data-retriever.git
    cd ncei-data-retriever/
    ```

2. Create and activate a [conda](https://conda.io/projects/conda/en/latest/index.html) environment:
    ```
    conda env create -f environment.yml
    conda activate ncei
    ```

3. You can now retrieve data:
    ```
    python ./scripts/retrieve_daily_summaries.py USC00351877 -s 2017-08-15 -e 2020-12-16 -p ./data
    ```

## Usage

The only data currently implemented are [NCEI Global Historical Climatology Network - Daily (GHCND) data](https://www.ncei.noaa.gov/pub/data/cdo/documentation/GHCND_documentation.pdf). This script can be used to access the "core" values: 
TMIN (daily minimum temperature), TMAX (daily minimum temperature), PRCP (daily total precipitation), SNOW (daily total snowfall), and SNWD (total snow depth)

Only a station ID is required. You can search for stations using the [NCEI Climate Data Online search tool](https://www.ncdc.noaa.gov/cdo-web/search).
Multiple stations can be included, separated by a space:
```
python ./scripts/retrieve_daily_summaries.py USC00351877 USW00014820
```

Find help using the `--help` flag:
```
python ./scripts/retrieve_daily_summaries.py --help
```
```
usage: retrieve_daily_summaries.py [-h] [-i info] [-s start] [-e end] [-p path] station

Retrieve daily summaries from NCEI data service API and save to netCDF file.

positional arguments:
  station               NCEI station ID (multiple stations can be separated by commas)

options:
  -h, --help            show this help message and exit
  -i info, --info info  Get station information only (doesn't retrieve data). Enter true or false (default: false).
  -s start, --start start
                        Start date in YYYY-MM-DD format (default: 1750-01-01)
  -e end, --end end     End date in YYYY-MM-DD format (default: 2024-06-14)
  -p path, --path path  Path for saving file (defaults to current directory)
```

## License

This package is licensed under the MIT License. See the [LICENSE](LICENSE) file for more information.
