import argparse
from io import StringIO
import os

import pandas as pd
import requests


def _parse_args():
    parser = argparse.ArgumentParser(
        description='Retrieve daily summaries from NCEI data service API and save to netCDF file.'
    )
    parser.add_argument('station', metavar='station', type=str, nargs='+',
                        help='NCEI station ID (multiple stations can be separated by spaces)')
    parser.add_argument('-i', '--info', action=argparse.BooleanOptionalAction,
                        help='Get station information only (default --no-info).')
    parser.add_argument('-a', '--all', action=argparse.BooleanOptionalAction,
                        help='Retrieve all available variables for station(s) (default --no-all).')
    parser.add_argument('-s', '--start', type=str, metavar='start', default='1750-01-01',
                        help='Start date in YYYY-MM-DD format (default: 1750-01-01)')
    parser.add_argument('-e', '--end', type=str, metavar='end', default=pd.Timestamp.now().strftime('%Y-%m-%d'),
                        help=f'End date in YYYY-MM-DD format (default: {pd.Timestamp.now().strftime("%Y-%m-%d")})')
    parser.add_argument('-p', '--path', type=str, metavar='path', default='.',
                        help='Path for saving file (defaults to current directory)')
    return parser.parse_args()


def _get_global_attrs(dataset, station, lon, lat, start_date, end_date, name):
    global_attrs = {
        'station': station,
        'station_name': name,
        'station_page': f"https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/stations/GHCND:{station}/detail",
        'station_longitude (deg E)': lon,
        'station_latitude (deg N)': lat,
        'dataset': dataset,
        'start_date (YYYY-MM-DD)': start_date,
        'end_date (YYYY-MM-DD)': end_date,
        'accessed (YYYY-MM-DD HH:MM:SS)': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
        'access_method': 'NCEI Data Service API (https://www.ncei.noaa.gov/support/access-data-service-api-user-documentation)',
    }
    return global_attrs


def _get_soil_temps():
    soil_temps = {}
    ground_cover = [
        'unknown',
        'grass',
        'fallow',
        'bare ground',
        'brome grass',
        'sod',
        'straw multch',
        'grass muck',
        'bare muck',
    ]
    depth_codes = [
        '5',
        '10',
        '20',
        '50',
        '100',
        '150',
        '180',
    ]
    for i, ground in enumerate(ground_cover):
        for j, depth in enumerate(depth_codes):
            soil_temps.update({
                f'sn{i:02d}{j+1:02d}': {
                    'units': 'degrees_Celsius',
                    'standard_name': 'soil_temperature',
                    'long_name': 'minimum soil temperature at {depth} depth under {ground_cover}',
                    'depth': f'{depth} cm',
                    'ground_cover': ground,
                    'cell_methods': 'time: minimum (interval: 1 day)'
                }
            })
            soil_temps.update({
                f'sx{i:02d}{j+1:02d}': {
                    'units': 'degrees_Celsius',
                    'standard_name': 'soil_temperature',
                    'long_name': 'maximum soil temperature at {depth} depth under {ground_cover}',
                    'depth': f'{depth} cm',
                    'ground_cover': ground,
                    'cell_methods': 'time: maximum (interval: 1 day)'
                }
            })
    return soil_temps


def _get_weather_types():
    weather_codes = {}
    weather_types = [
        'Fog, ice fog, or freezing fog (may include heavy fog)',
        'Heavy fog or heaving freezing fog (not always distinguished from fog)',
        'Thunder',
        'Ice pellets, sleet, snow pellets, or small hail',
        'Hail (may include small hail)',
        'Glaze or rime',
        'Dust, volcanic ash, blowing dust, blowing sand, or blowing obstruction',
        'Smoke or haze',
        'Blowing or drifting snow',
        'Tornado, waterspout, or funnel cloud',
        'High or damaging winds',
        'Blowing spray',
        'Mist',
        'Drizzle',
        'Freezing drizzle',
        'Rain (may include freezing rain, drizzle, and freezing drizzle)',
        'Freezing rain',
        'Snow, snow pellets, snow grains, or ice crystals',
        'Unknown source of precipitation',
        'not used',
        'Ground fog',
        'Ice fog or freezing fog',
    ]
    for i in range(1, 23):
        weather_codes.update({
            f'wt{i:02d}': {
                'long_name': 'weather type',
                'type': weather_types[i-1],
            }
        })
        if i == 1 or i == 3 or i == 7 or i == 18 or i == 20:
            weather_codes.update({
                f'wv{i:02d}': {
                    'long_name': 'weather in the vincinity',
                    'type': weather_types[i-1],
                }
            })
    return weather_codes


def _get_data_attrs(all_vars=False):
    data_attrs = {
        'tmin': {
            'units': 'degrees_Celsius',
            'standard_name': 'air_temperature',
            'long_name': 'daily minimum air temperature',
            'cell_methods': 'time: minimum (interval: 1 day)',
            'units_metadata': 'on-scale',
        },
        'tmax': {
            'units': 'degrees_Celsius',
            'standard_name': 'air_temperature',
            'long_name': 'daily maximum air temperature',
            'cell_methods': 'time: maximum (interval: 1 day)',
            'units_metadata': 'on-scale',
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
    if all_vars:
        data_attrs.update({
            'acmc': {
                'units': 'percent',
                'standard_name': 'cloud_area_fraction',
                'long_name': 'average cloudiness midnight to midnight',
                'cell_methods': 'time: mean (interval: 1 day)',
                'method': 'ceilometer',
            },
            'acmh': {
                'units': 'percent',
                'standard_name': 'cloud_area_fraction',
                'long_name': 'average cloudiness midnight to midnight',
                'cell_methods': 'time: mean (interval: 1 day)',
                'method': 'manual',
            },
            'acsc': {
                'units': 'percent',
                'standard_name': 'cloud_area_fraction',
                'long_name': 'average cloudiness sunrise to sunset',
                'method': 'ceilometer',
            },
            'acsh': {
                'units': 'percent',
                'standard_name': 'cloud_area_fraction',
                'long_name': 'average cloudiness sunrise to sunset',
                'method': 'manual',
            },
            'adpt': {
                'units': 'degrees_Celsius',
                'standard_name': 'dew_point_temperature',
                'long name': 'average dew point temperature for the day',
                'cell_methods': 'time: mean (interval: 1 day)',
                'units_metadata': 'on-scale',
            },
            'aslp': {
                'units': 'hPa',
                'standard_name': 'air_pressure_at_sea_level',
                'long_name': 'average sea level pressure for the day',
                'cell_methods': 'time: mean (interval: 1 day)',
            },
            'astp': {
                'units': 'hPa',
                'standard_name': 'air_pressure',
                'long_name': 'average station pressure for the day',
                'cell_methods': 'time: mean (interval: 1 day)',
            },
            'awbt': {
                'units': 'degrees_Celsius',
                'standard_name': 'wet_bulb_temperature',
                'long_name': 'average wet bulb temperature for the day',
                'cell_methods': 'time: mean (interval: 1 day)',
                'units_metadata': 'on-scale',
            },
            'awdr': {
                'units': 'degrees',
                'standard_name': 'wind_from_direction',
                'long_name': 'average wind direction for the day',
                'cell_methods': 'time: mean (interval: 1 day)',
                'units_metadata': 'direction_cw_from_north',
            },
            'awnd': {
                'units': 'm/s',
                'standard_name': 'wind_speed',
                'long_name': 'average wind speed for the day',
                'cell_methods': 'time: mean (interval: 1 day)',
            },
            'daev': {
                'units': 'days',
                'standard_name': 'number_of_days_in_evaporation_observation',
                'long_name': 'number of days included in the multiday evaporation total (mdev)',
            },
            'dapr': {
                'units': 'days',
                'standard_name': 'number_of_days_in_precipitation_observation',
                'long_name': 'number of days included in the multiday precipitation total (mdpr)',
            },
            'dasf': {
                'units': 'days',
                'standard_name': 'number_of_days_in_snowfall_observation',
                'long_name': 'number of days included in the multiday snowfall total (mdsf)',
            },
            'datn': {
                'units': 'days',
                'standard_name': 'number_of_days_in_min_temperature_observation',
                'long_name': 'number of days included in the multiday minimum temperature (mdtn)',
            },
            'datx': {
                'units': 'days',
                'standard_name': 'number_of_days_in_max_temperature_observation',
                'long_name': 'number of days included in the multiday maximum temperature (mdtx)',
            },
            'dawm': {
                'units': 'days',
                'standard_name': 'number_of_days_in_wind_movement_observation',
                'long_name': 'number of days included in the multiday wind movement (mdwm)',
            },
            'dwpr': {
                'units': 'days',
                'standard_name': 'number_of_days_with_nonzero_precipitation_in_precipitation_observation',
                'long_name': 'number of days with non-zero precipitation included in the multiday precipitation total (mdpr)',
            },
            'evap': {
                'units': 'mm',
                'standard_name': 'lwe_thickness_of_water_evaporation_amount',
                'long_name': 'daily total evaporation from evaporation pan',
                'cell_methods': 'time: sum (interval: 1 day)',
            },
            'fmtm': {
                'units': 'time_of_day',
                'standard_name': 'time_of_fastest_1min_wind',
                'long_name': 'time of fastest 1-minute wind speed',
                'units_metadata': 'time_format_HHMM',
            },
            'frgb': {
                'units': 'cm',
                'standard_name': 'depth_at_base_of_unfrozen_ground',
                'long_name': 'depth of base of frozen ground layer',
            },
            'frgt': {
                'units': 'cm',
                'standard_name': 'depth_at_top_of_frozen_ground_layer',
                'long_name': 'depth of top of frozen ground layer',
            },
            'frth': {
                'units': 'cm',
                'standard_name': 'lwe_thickness_of_frozen_water_content_of_soil_layer',
                'long_name': 'thickness of frozen ground layer',
            },
            'gaht': {
                'units': 'cm',
                'standard_name': 'difference_between_river_and_gauge_height',
                'long_name': 'difference between river and gauge height',
            },
            'mdev': {
                'units': 'mm',
                'standard_name': 'lwe_thickness_of_water_evaporation_amount',
                'long_name': 'multiday evaporation total (use with daev)',
                'cell_methods': 'time: sum (interval: see variable daev)',
            },
            'mdpr': {
                'units': 'mm',
                'standard_name': 'lwe_thickness_of_precipitation_amount',
                'long_name': 'multiday precipitation total (use with dapr)',
                'cell_methods': 'time: sum (interval: see variable dapr)',
            },
            'mdsf': {
                'units': 'mm',
                'standard_name': 'lwe_thickness_of_snowfall_amount',
                'long_name': 'multiday snowfall total (use with dasf)',
                'cell_methods': 'time: sum (interval: see variable dasf)',
            },
            'mdtn': {
                'units': 'degrees_Celsius',
                'standard_name': 'air_temperature',
                'long_name': 'multiday minimum temperature (use with datn)',
                'cell_methods': 'time: minimum (interval: see variable datn)',
            },
            'mdtx': {
                'units': 'degrees_Celsius',
                'standard_name': 'air_temperature',
                'long_name': 'multiday maximum temperature (use with datx)',
                'cell_methods': 'time: maximum (interval: see variable datx)',
            },
            'mdwm': {
                'units': 'km',
                'standard_name': 'wind_movement',
                'long_name': 'multiday wind movement (use with dawm)',
                'cell_methods': 'time: sum (interval: see variable dawm)',
            },
            'mnpn': {
                'units': 'degrees_Celsius',
                'standard_name': 'temperature_in_evaporation_pan',
                'long_name': 'minimum temperature in evaporation pan',
                'cell_methods': 'time: minimum (interval: 1 day)',
            },
            'mxpn': {
                'units': 'degrees_Celsius',
                'standard_name': 'temperature_in_evaporation_pan',
                'long_name': 'maximum temperature in evaporation pan',
                'cell_methods': 'time: maximum (interval: 1 day)',
            },
            'pgtm': {
                'units': 'time_of_day',
                'standard_name': 'time_of_peak_wind_gust',
                'long_name': 'time of peak wind gust',
                'units_metadata': 'time_format_HHMM',
            },
            'psun': {
                'units': 'percent',
                'standard_name': 'sunshine_fraction',
                'long_name': 'percent of possible sunshine',
            },
            'rhav': {
                'units': 'percent',
                'standard_name': 'relative_humidity',
                'long_name': 'average relative humidity for the day',
                'cell_methods': 'time: mean (interval: 1 day)',
            },
            'rhmn': {
                'units': 'percent',
                'standard_name': 'relative_humidity',
                'long_name': 'minimum relative humidity for the day',
                'cell_methods': 'time: minimum (interval: 1 day)',
            },
            'rhmx': {
                'units': 'percent',
                'standard_name': 'relative_humidity',
                'long_name': 'maximum relative humidity for the day',
                'cell_methods': 'time: maximum (interval: 1 day)',
            },
            'taxn': {
                'units': 'degrees_Celsius',
                'standard_name': 'air_temperature',
                'long_name': 'average temperature for the day computed as tmax + tmin / 2',
                'cell_methods': 'time: mean (interval: 1 day)',
            },
            'tavg': {
                'units': 'degrees_Celsius',
                'standard_name': 'air_temperature',
                'long_name': 'average temperature for the day from hourly observations',
                'cell_methods': 'time: mean (interval: 1 day)',
            },
            'thic': {
                'units': 'mm',
                'standard_name': 'floating_ice_thickness',
                'long_name': 'thickness of ice on water',
            },
            'tobs': {
                'units': 'degrees_Celsius',
                'standard_name': 'air_temperature',
                'long_name': 'temperature at the time of observation',
            },
            'tsun': {
                'units': 'minutes',
                'standard_name': 'sunshine_duration',
                'long_name': 'total sunshine duration for the day',
                'cell_methods': 'time: sum (interval: 1 day)',
            },
            'wdf1': {
                'units': 'degrees',
                'standard_name': 'wind_from_direction',
                'long_name': 'direction of fastest 1-minute wind',
                'units_metadata': 'direction_cw_from_north',
            },
            'wdf2': {
                'units': 'degrees',
                'standard_name': 'wind_from_direction',
                'long_name': 'direction of fastest 2-minute wind',
                'units_metadata': 'direction_cw_from_north',
            },
            'wdf5': {
                'units': 'degrees',
                'standard_name': 'wind_from_direction',
                'long_name': 'direction of fastest 5-minute wind',
                'units_metadata': 'direction_cw_from_north',
            },
            'wdfg': {
                'units': 'degrees',
                'standard_name': 'wind_from_direction',
                'long_name': 'direction of peak gust',
                'units_metadata': 'direction_cw_from_north',
            },
            'wdfi': {
                'units': 'degrees',
                'standard_name': 'wind_from_direction',
                'long_name': 'direction of highest instantaneous wind',
                'units_metadata': 'direction_cw_from_north',
            },
            'wdfm': {
                'units': 'degrees',
                'standard_name': 'wind_from_direction',
                'long_name': 'fastest mile wind direction',
                'units_metadata': 'direction_cw_from_north',
            },
            'wdmv': {
                'units': 'km',
                'standard_name': 'wind_movement',
                'long_name': 'wind movement for the day',
                'cell_methods': 'time: sum (interval: 1 day)',
            },
            'wesd': {
                'units': 'mm',
                'standard_name': 'lwe_thickness_of_surface_snow_amount',
                'long_name': 'water equivalent of snow on the ground',
            },
            'wesf': {
                'units': 'mm',
                'standard_name': 'lwe_thickness_of_snowfall_amount',
                'long_name': 'water equivalent of snowfall',
            },
            'wsf1': {
                'units': 'm/s',
                'standard_name': 'wind_speed',
                'long_name': 'fastest 1-minute wind speed',
            },
            'wsf2': {
                'units': 'm/s',
                'standard_name': 'wind_speed',
                'long_name': 'fastest 2-minute wind speed',
            },
            'wsf5': {
                'units': 'm/s',
                'standard_name': 'wind_speed',
                'long_name': 'fastest 5-minute wind speed',
            },
            'wsfg': {
                'units': 'm/s',
                'standard_name': 'wind_speed',
                'long_name': 'peak gust wind speed',
            },
            'wsfi': {
                'units': 'm/s',
                'standard_name': 'wind_speed',
                'long_name': 'highest instantaneous wind speed',
            },
            'wsfm': {
                'units': 'm/s',
                'standard_name': 'wind_speed',
                'long_name': 'fastest mile wind speed',
            },
        })
        data_attrs.update(_get_weather_types())
        data_attrs.update(_get_soil_temps())
    return data_attrs


def _construct_url(dataset, station, start_date, end_date, search=False):
    if search:
        url = f"https://www.ncei.noaa.gov/access/services/search/v1/data?dataset={dataset}&stations={station}&available=true&startDate={start_date}&endDate={end_date}&format=json"
    else:
        url = f"https://www.ncei.noaa.gov/access/services/data/v1?dataset={dataset}&stations={station}&startDate={start_date}&endDate={end_date}&format=csv"
    return url


def _check_station_data(station, dataset, start_date, end_date, all_vars=False):
    # definitely need to find a better way to break this one up
    # core elements for daily summaries dataset
    # see also https://www.ncei.noaa.gov/pub/data/cdo/documentation/GHCND_documentation.pdf
    if not all_vars:
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
            if not all_vars:
                vars_in_dataset = [var for var in vars if var in data_type_ids]
            else:
                vars_in_dataset = data_type_ids
                vars = vars_in_dataset
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
            name = data['results'][0]['stations'][0]['name']
            station_start_date = data['results'][0]['startDate']
            if pd.to_datetime(start_date) < pd.to_datetime(station_start_date):
                start_date = station_start_date.split('T')[0]
                print(f"Adjusted start date to {start_date} at station {station} based on available data.")
            station_end_date = data['results'][0]['endDate']
            if pd.to_datetime(end_date) > pd.to_datetime(station_end_date):
                end_date = station_end_date.split('T')[0]
                print(f"Adjusted end date to {end_date} at station {station} based on available data.")
            return vars_in_dataset, lon, lat, start_date, end_date, name
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

    stations = args['station']
    info = args['info']
    all_vars = args['all']
    requested_start_date = args['start']
    requested_end_date = args['end']
    path = args['path']

    try:
        pd.to_datetime(requested_start_date, format='%Y-%m-%d')
        pd.to_datetime(requested_end_date, format='%Y-%m-%d')
    except ValueError:
        raise ValueError("Invalid date. Please use YYYY-MM-DD format.")

    if info:
        for station in stations:
            # get variables and station info for requested station and date range
            try:
                vars, lon, lat, start_date, end_date, name = _check_station_data(
                    station, dataset, requested_start_date, requested_end_date, all_vars=all_vars
                )
            except TypeError:
                continue
            print(f"Station: {station}")
            print(f"Station name: {name}")
            print(f"Variables: {', '.join(vars)}")
            print(f"Longitude: {lon}")
            print(f"Latitude: {lat}")
            print(f"Start date: {start_date}")
            print(f"End date: {end_date}")
            print(f"Site URL: https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/stations/GHCND:{station}/detail")
    else:
        for station in stations:
            # get variables and station info for requested station and date range
            try:
                # a bit of a hack, function returns None if station or date range is invalid,
                # so it will thrown a TypeError when trying to unpack NoneType object
                vars, lon, lat, start_date, end_date, name = _check_station_data(
                    station, dataset, requested_start_date, requested_end_date, all_vars=all_vars
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
            data.attrs = _get_global_attrs(dataset, station, lon, lat, start_date, end_date, name)
            for s in data:
                data[s].attrs = _get_data_attrs(all_vars=all_vars)[s]

            # save to netcdf
            if not os.path.exists(path):
                os.makedirs(path)
            if all_vars:
                save_path = os.path.join(path, f'{station}_{dataset}_{start_date}_{end_date}_all-vars.nc')
            else:
                save_path = os.path.join(path, f'{station}_{dataset}_{start_date}_{end_date}_core-vars.nc')
            data.to_netcdf(save_path)
            save_path = save_path.replace('\\', '\\\\')  # escape backslashes for Windows paths
            print(f"Saved data to {save_path}")
