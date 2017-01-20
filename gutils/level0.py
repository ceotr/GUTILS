import numpy as np

from gutils.gbdr import (
    GliderBDReader,
    MergedGliderBDReader
)
from gutils.yo import find_yo_extrema
from gutils.gps import interpolate_gps
from gutils.yo.filters import default_filter

from gutils.nc import open_glider_netcdf, GLIDER_UV_DATATYPE_KEYS


def create_reader(flight_path, science_path):
    if flight_path is not None:
        flight_reader = GliderBDReader(
            [flight_path]
        )
        if science_path is None:
            return flight_reader
    if science_path is not None:
        science_reader = GliderBDReader(
            [science_path]
        )
        if flight_path is None:
            return science_reader
    return MergedGliderBDReader(flight_reader, science_reader)


def find_profiles(flight_path, science_path, time_name, depth_name):
    profile_values = []
    reader = create_reader(flight_path, science_path)
    for line in reader:
        if depth_name in line:
            profile_values.append([line[time_name], line[depth_name]])

    if not profile_values:
        raise ValueError('Not enough profiles found')

    try:
        profile_values = np.array(profile_values)
        timestamps = profile_values[:, 0]
        depths = profile_values[:, 1]
    except IndexError:
        raise ValueError('Not enough timestamps or depths found')
    else:
        profile_dataset = find_yo_extrema(timestamps, depths)
        return default_filter(profile_dataset)


def get_file_set_gps(flight_path, science_path, time_name, gps_prefix):
    gps_values = []
    reader = create_reader(flight_path, science_path)
    lat_name = gps_prefix + 'lat-lat'
    lon_name = gps_prefix + 'lon-lon'
    for line in reader:
        if lat_name in line:
            gps_values.append(
                [line[time_name], line[lat_name], line[lon_name]]
            )
        else:
            gps_values.append([line[time_name], np.nan, np.nan])

    if not gps_values:
        raise ValueError('Not enough gps posistions found')
    try:
        gps_values = np.array(gps_values)
        timestamps = gps_values[:, 0]
        latitudes = gps_values[:, 1]
        longitudes = gps_values[:, 2]
    except IndexError:
        raise ValueError('Not enough timestamps, latitudes, or longitudes found')
    else:
        gps_values[:, 1], gps_values[:, 2] = interpolate_gps(
            timestamps, latitudes, longitudes
        )

    return gps_values


def fill_gps(line, interp_gps, time_name, gps_prefix):
    lat_name = gps_prefix + 'lat-lat'
    lon_name = gps_prefix + 'lon-lon'
    if lat_name not in line:
        timestamp = line[time_name]
        line[lat_name] = interp_gps[interp_gps[:, 0] == timestamp, 1][0]
        line[lon_name] = interp_gps[interp_gps[:, 0] == timestamp, 2][0]

    return line


def fill_uv_variables(dst_glider_nc, uv_values):
    for key, value in uv_values.items():
        dst_glider_nc.set_scalar(key, value)


def backfill_uv_variables(src_glider_nc, empty_uv_processed_paths):
    uv_values = {}
    for key_name in GLIDER_UV_DATATYPE_KEYS:
        uv_values[key_name] = src_glider_nc.get_scalar(key_name)

    for file_path in empty_uv_processed_paths:
        with open_glider_netcdf(file_path, 'a') as dst_glider_nc:
            fill_uv_variables(dst_glider_nc, uv_values)

    return uv_values


def init_netcdf(file_path, attrs, segment_id, profile_id):
    with open_glider_netcdf(file_path, 'w') as glider_nc:
        # Set global attributes
        glider_nc.set_global_attributes(attrs['global'])

        # Set Trajectory
        glider_nc.set_trajectory_id(
            attrs['deployment']['glider'],
            attrs['deployment']['trajectory_date']
        )

        # Set Platform
        glider_nc.set_platform(attrs['deployment']['platform'])

        # Set Instruments
        glider_nc.set_instruments(attrs['instruments'])

        # Set Segment ID
        glider_nc.set_segment_id(segment_id)

        # Set Profile ID
        glider_nc.set_profile_id(profile_id)
