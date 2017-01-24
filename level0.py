#!/usr/bin/python
# TODO: write tests
from gutils.gbdr import (
    GliderBDReader,
    MergedGliderBDReader
)

import datetime

import os
import glob
import shutil
import tempfile

import sensor_tracker_interface


import numpy as np
import netCDF4 as nc4

# from gutils.nc import open_glider_netcdf
from sensor_tracker_interface import open_glider_netcdf
from gutils.scripts.create_glider_netcdf import process_dataset
from gutils.yo import find_yo_extrema
from gutils.gps import interpolate_gps
from gutils.yo.filters import default_filter
from gutils.gbdr.methods import parse_glider_filename
from gutils.level0 import *


def pair_files(flight_list, science_list):
    paired_files = []
    science_names = {}
    for f in science_list:
        name = os.path.split(f)[1].split('.')[0]
        science_names[name] = f
    for f in flight_list:
        name = os.path.split(f)[1].split('.')[0]
        pair = [f]
        if name in science_names:
            pair.append(science_names[name])
        paired_files.append(pair)
    return paired_files


if __name__ == '__main__':
    raw_data_path = '/home/bcovey/full_res_data/subset/'
    platform = 'otn200'
    start_time = datetime.datetime.strptime('2017-01-19 00:00:00', '%Y-%m-%d %H:%M:%S')

    flight_files = sorted(glob.glob(os.path.join(raw_data_path, '*.DBD')))

    science_files = sorted(glob.glob(os.path.join(raw_data_path, '*.EBD')))

    sorted_files = pair_files(flight_files, science_files)

    flightReader = GliderBDReader([sorted_files[0][0]])
    scienceReader = GliderBDReader([sorted_files[0][1]])
    reader = MergedGliderBDReader(flightReader, scienceReader)

    import pprint
    pp = pprint.PrettyPrinter(indent=4)
    print("Instruments in data:")
    pp.pprint(sorted(list(sensor_tracker_interface.group_headers(reader.headers).keys())))

    with sensor_tracker_interface.SensorTrackerInterface() as tracker_interface:
        tracker_interface.update_instruments_in_metadata_db(reader, platform)
        json = tracker_interface.get_json_format_for_deployment(
            platform,
            datetime.datetime.strptime('2017-01-19 00:00:00', '%Y-%m-%d %H:%M:%S')
        )
        pp.pprint(json)
    # import pprint
    # pp = pprint.PrettyPrinter(indent=4)
    # pp.pprint(sorted(list(group_headers(reader.headers).keys())))

    nc_dir = '../nc/'
    for i, pair in enumerate(sorted_files):
        attrs = json
        timestr = 'timestamp'

        flight_path = pair[0]
        science_path = pair[1]

        glider_name = attrs['deployment']['glider']
        deployment_name = '{}-{}'.format(
            glider_name,
            attrs['deployment']['trajectory_date']
        )

        try:
            # Find profile breaks
            profiles = find_profiles(flight_path, science_path, 'timestamp', 'm_depth-m')

            # Interpolate GPS
            interp_gps = get_file_set_gps(
                flight_path, science_path, timestr, 'm_gps_'
            )

            interp_time = get_file_set_timestamps(
                flight_path,
                science_path,
                'm_present_time-timestamp',
                'sci_m_present_time-timestamp',
                'm_science_clothesline_lag-s'
            )
        except ValueError as e:
            print('{} - Skipping'.format(e))
            print('Skipping: %s' % (i + 1))
            continue
        print("Not skipping: %s" % (i + 1))

        # Create NetCDF Files for Each Profile
        profile_id = 0
        profile_end = 0
        file_path = None
        uv_values = None
        movepairs = []
        empty_uv_processed_paths = []
        reader = create_reader(flight_path, science_path)

        # Tempdirectory
        tmpdir = tempfile.mkdtemp()

        for line in reader:
            if profile_end < line[timestr]:
                # New profile! init the NetCDF output file

                # Path to hold file while we create it
                _, tmp_path = tempfile.mkstemp(dir=tmpdir, suffix='.nc', prefix='gutils')

                # Open new NetCDF
                begin_time = datetime.datetime.utcfromtimestamp(line[timestr])
                filename = "%s_%s_%s.nc" % (
                    glider_name,
                    begin_time.strftime("%Y%m%dT%H%M%SZ"),
                    'delayed'
                )

                file_path = os.path.join(
                    nc_dir,
                    deployment_name,
                    filename
                )

                # NOTE: Store 1 based profile id
                init_netcdf(tmp_path, attrs, i + 1, profile_id + 1)
                profile = profiles[profiles[:, 2] == profile_id]
                if len(profile) < 1:
                    continue
                profile_end = max(profile[:, 0])

            with sensor_tracker_interface.open_glider_netcdf(tmp_path, platform, start_time, 'a') as glider_nc:
                while line[timestr] <= profile_end:
                    line = fill_gps(line, interp_gps, 'timestamp', 'm_gps_')
                    line = fill_timestamp(line, interp_time, 'sci_m_present_time-timestamp')

                    glider_nc.stream_dict_insert(line)
                    try:
                        line = reader.__next__()
                    except StopIteration:
                        break

                # Handle UV Variables
                if glider_nc.contains('time_uv'):
                    uv_values = backfill_uv_variables(
                        glider_nc, empty_uv_processed_paths
                    )
                elif uv_values is not None:
                    fill_uv_variables(glider_nc, uv_values)
                    del empty_uv_processed_paths[:]
                else:
                    empty_uv_processed_paths.append(tmp_path)

                glider_nc.update_profile_vars()
                try:
                    glider_nc.calculate_salinity()
                    glider_nc.calculate_density()
                except BaseException as e:
                    print(e)
                glider_nc.update_bounds()

            movepairs.append((tmp_path, file_path))

            profile_id += 1

        for tp, fp in movepairs:
            try:
                os.makedirs(os.path.dirname(fp))
            except OSError:
                pass  # destination folder exists
            shutil.move(tp, fp)
        shutil.rmtree(tmpdir)
