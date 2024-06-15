import argparse
from io import StringIO
import os

import pandas as pd
import requests


def _parse_args():
    parser = argparse.ArgumentParser(
        description='Retrieve daily summaries from NCEI data service API and save to netCDF file.'
    )
    parser.add_argument('station', metavar='station', type=str,
                        help='NCEI station ID (multiple stations can be separated by commas)')
    parser.add_argument('-i', '--info', metavar='info', type=str, default='false',
                        help='Get station information only (doesn\'t retrieve data). Enter true or false (default: false).')
    parser.add_argument('-s', '--start', type=str, metavar='start', default='1750-01-01',
                        help='Start date in YYYY-MM-DD format (default: 1750-01-01)')
    parser.add_argument('-e', '--end', type=str, metavar='end', default=pd.Timestamp.now().strftime('%Y-%m-%d'),
                        help=f'End date in YYYY-MM-DD format (default: {pd.Timestamp.now().strftime("%Y-%m-%d")})')
    parser.add_argument('-p', '--path', type=str, metavar='path', default='.',
                        help='Path for saving file (defaults to current directory)')
    return parser.parse_args()


def _get_global_attrs(dataset, station, lon, lat, start_date, end_date):
    global_attrs = {
        'station': station,
        'station_longitude (deg E)': lon,
        'station_latitude (deg N)': lat,
        'dataset': dataset,
        'start_date (YYYY-MM-DD)': start_date,
        'end_date (YYYY-MM-DD)': end_date,
        'accessed (YYYY-MM-DD HH:MM:SS)': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
        'access_method': 'NCEI Data Service API (https://www.ncei.noaa.gov/support/access-data-service-api-user-documentation)',
    }
    return global_attrs


def _get_data_attrs():
    data_attrs = {
        'tmin': {
            'units': 'degrees Celsius',
            'standard_name': 'air_temperature',
            'long_name': 'daily minimum air temperature',
            'cell_methods': 'time: minimum (interval: 1 day)',
        },
        'tmax': {
            'units': 'degrees Celsius',
            'standard_name': 'air_temperature',
            'long_name': 'daily maximum air temperature',
            'cell_methods': 'time: maximum (interval: 1 day)',
        },
        'prcp': {
            'units': 'mm',
            'standard_name': 'lwe_thickness_of_precipitation_amount',
            'long_name': 'daily total precipitation',
            'cell_methods': 'time: sum (interval: 1 day)',
        },
        'snow': {
            'units': 'mm',
            'standard_name': 'thickness_of_snowfall_amount',
            'long_name': 'daily total snowfall',
            'cell_methods': 'time: sum (interval: 1 day)',
        },
        'snwd': {
            'units': 'mm',
            'standard_name': 'surface_snow_thickness',
            'long_name': 'daily snow depth',
            'cell_methods': 'time: point',
        },
    }
    return data_attrs


def _construct_url(dataset, station, start_date, end_date, search=False):
    if search:
        url = f"https://www.ncei.noaa.gov/access/services/search/v1/data?dataset={dataset}&stations={station}&available=true&startDate={start_date}&endDate={end_date}&format=json"
    else:
        url = f"https://www.ncei.noaa.gov/access/services/data/v1?dataset={dataset}&stations={station}&startDate={start_date}&endDate={end_date}&format=csv"
    return url


def _check_station_data(station, dataset, start_date, end_date):
    # definitely need to find a better way to break this one up
    # core elements for daily summaries dataset
    # see also https://www.ncei.noaa.gov/pub/data/cdo/documentation/GHCND_documentation.pdf
    vars = ['TMIN', 'TMAX', 'PRCP', 'SNOW', 'SNWD']

    # check if station exists and has data for any dates
    url = _construct_url(dataset, station, '1750-01-01', pd.Timestamp.now().strftime("%Y-%m-%d"), search=True)
    request = requests.get(url, timeout=60)
    if request.status_code == 200:
        data = request.json()
        if len(data['results']) == 0:
            print(f"No data available for {station}. Check station ID.")
            return None
    elif request.status_code == 500:
        print(f"Failed to retrieve data from {station}.")
        print(f"{request.status_code}: {request.json()['errorMessage']}")
        print(f"{request.json()['errors']['message']}")
    else:
        print(f"Failed to retrieve data from {station}.")
        print(f"{request.status_code}: {request.json()['errorMessage']}")
        if 'errors' in request.json():
            for error in request.json()['errors']:
                field_dict = {
                    'startDate': 'start',
                    'endDate': 'end',
                }
                if error['field'] in field_dict:
                    print(f"Error in field {field_dict[error['field']]}: check input argument(s) '{field_dict[error['field']]}' and try again.")
                else:
                    print(f"Error in field {error['field']}: {error['message']}. Check station ID.")
        return None

    # check if station has data for specified date range
    url = _construct_url(dataset, station, start_date, end_date, search=True)
    request = requests.get(url, timeout=60)
    if request.status_code == 200:
        data = request.json()
        if len(data['results']) == 0:
            print(f"No data available for {station} over dates {start_date}-{end_date}. Check date range.")
            return None
        else:
            # otherwise, get available variables for station and date range
            data_type_ids = [d['id'] for d in data['results'][0]['dataTypes']]
            vars_in_dataset = [var for var in vars if var in data_type_ids]
            if len(vars_in_dataset) == 0:
                print(f"The station exists {station} and has data, but no core elements available.")
                print("Core elements include: TMIN, TMAX, PRCP, SNOW, SNWD")
                print("You may want to review the station page to find available data:")
                print(f"https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/stations/GHCND:{station}/detail")
                return None
            if len(vars_in_dataset) < len(vars):
                not_found = [var for var in vars if var not in vars_in_dataset]
                print(f"Elements {', '.join(not_found)} not available at {station} for dates {start_date}-{end_date}.")
                print(f"Available elements: {', '.join(vars_in_dataset)}")
            lon, lat = data['results'][0]['location']['coordinates']
            station_start_date = data['results'][0]['startDate']
            if pd.to_datetime(start_date) < pd.to_datetime(station_start_date):
                start_date = station_start_date.split('T')[0]
                print(f"Adjusted start date to {start_date} at station {station} based on available data.")
            station_end_date = data['results'][0]['endDate']
            if pd.to_datetime(end_date) > pd.to_datetime(station_end_date):
                end_date = station_end_date.split('T')[0]
                print(f"Adjusted end date to {end_date} at station {station} based on available data.")
            return vars_in_dataset, lon, lat, start_date, end_date
    elif request.status_code == 500:
        print(f"Failed to retrieve data from {station}.")
        print(f"{request.status_code}: {request.json()['errorMessage']}")
        print(f"{request.json()['errors']['message']}. Check date range.")
    else:
        print(f"Failed to retrieve data from {station}.")
        print(f"{request.status_code}: {request.json()['errorMessage']}")
        if 'errors' in request.json():
            for error in request.json()['errors']:
                field_dict = {
                    'startDate': 'start',
                    'endDate': 'end',
                }
                if error['field'] in field_dict:
                    print(f"Error in field {field_dict[error['field']]}: check input argument(s) '{field_dict[error['field']]}' and try again.")
                else:
                    print(f"Error in field {error['field']}: {error['message']}")
        return None


if __name__ == '__main__':
    dataset = "daily-summaries"

    args = _parse_args()
    args = vars(args)

    stations = args['station'].split(',')
    info = args['info']
    requested_start_date = args['start']
    requested_end_date = args['end']
    path = args['path']

    if info != 'false':
        for station in stations:
            # get variables and station info for requested station and date range
            try:
                vars, lon, lat, start_date, end_date = _check_station_data(
                    station, dataset, requested_start_date, requested_end_date
                )
            except TypeError:
                continue
            print(f"Station: {station}")
            print(f"Variables: {', '.join(vars)}")
            print(f"Longitude: {lon}")
            print(f"Latitude: {lat}")
            print(f"Start date: {start_date}")
            print(f"End date: {end_date}")
            print(f"Site URL: https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/stations/GHCND:{station}/detail")
    elif info == 'false':
        for station in stations:
            # get variables and station info for requested station and date range
            try:
                # a bit of a hack, function returns None if station or date range is invalid,
                # so it will thrown a TypeError when trying to unpack NoneType object
                vars, lon, lat, start_date, end_date = _check_station_data(
                    station, dataset, requested_start_date, requested_end_date
                )
            except TypeError:
                continue

            # retrieve data
            url = _construct_url(dataset, station, start_date, end_date)
            print(f"Retrieving data from {station}...", end="", flush=True)
            request = requests.get(url, timeout=60)
            if request.status_code != 200:
                print("")
                print(f"Failed to retrieve data from {station}.")
                print(f"{request.status_code}: {request.json()['errorMessage']}")
                if 'errors' in request.json():
                    for error in request.json()['errors']:
                        field_dict = {
                            'startDate': 'start',
                            'endDate': 'end',
                        }
                        if error['field'] in field_dict:
                            print(f"Error in field {field_dict[error['field']]}: check input argument(s) '{field_dict[error['field']]}' and try again.")
                        else:
                            print(f"Error in field {error['field']}: {error['message']}")
                continue
            else:
                print("done.")
            data = pd.read_csv(StringIO(request.text))
            if data.empty:
                print(f"No data available for {station}. Check station ID and date range.")
                continue

            # clean up data
            vars = ['DATE'] + vars
            data = data[vars]
            data['DATE'] = pd.to_datetime(data['DATE'])
            data.set_index('DATE', inplace=True)
            data = data.to_xarray()
            data = data.rename({'DATE': 'date'})
            data = data.rename({s: s.lower() for s in list(data.keys())})

            # add metadata
            data.attrs = _get_global_attrs(dataset, station, lon, lat, start_date, end_date)
            for s in data:
                data[s].attrs = _get_data_attrs()[s]

            # save to netcdf
            if not os.path.exists(path):
                os.makedirs(path)
            data.to_netcdf(os.path.join(path, f'{station}_{dataset}_{start_date}_{end_date}.nc'))
            print(f"Saved data to {os.path.join(path, f'{station}_{dataset}_{start_date}_{end_date}.nc')}")
