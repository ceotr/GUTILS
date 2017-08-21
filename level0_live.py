#!/usr/bin/python
# TODO: write tests
from gutils.gbdr import (
    GliderBDReader,
    MergedGliderBDReader
)

import datetime
import pickle

import os
import sys
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


def main():
    with sensor_tracker_interface.SensorTrackerInterface() as tracker_interface:
        deployments = tracker_interface.get_active_deployments()
    # (glider_name, mission_start yyyy-mm-dd)
    base_path = '/var/opt/gmc_gliderbak/default/gliders/%s/from-glider'

    nc_dir = '/home/slocum/netcdf'

    processing_cache_file = '/home/slocum/already_processed.pkl'

    if os.path.isfile(processing_cache_file):
        with open(processing_cache_file, 'rb') as f:
            already_processed = pickle.load(f)
    else:
        already_processed = {}

    for res in deployments:
        deployment = res[0]
        platform = res[1]
        platform_name = platform.name

        raw_data_path = base_path % (
            platform.name.lower()
        )

        start_time = deployment.start_time

        files = os.listdir(raw_data_path)
        flight_files = []
        science_files = []
        for f in sorted(files):
            if len(f.split('-')) > 1:
                if '.sbd' in f.lower():
                    flight_files.append(os.path.join(raw_data_path, f))
                elif '.tbd' in f.lower():
                    science_files.append(os.path.join(raw_data_path, f))

        sorted_files = pair_files(flight_files, science_files)

        try:
            flightReader = GliderBDReader([sorted_files[0][0]])
            scienceReader = GliderBDReader([sorted_files[0][1]])
            reader = MergedGliderBDReader(flightReader, scienceReader)
        except ValueError:
            print(flight_files)
            print(raw_data_path)
            raise

        import pprint
        pp = pprint.PrettyPrinter(indent=4)
        print("Instruments in data:")
        pp.pprint(sorted(list(sensor_tracker_interface.group_headers(reader.headers).keys())))

        with sensor_tracker_interface.SensorTrackerInterface() as tracker_interface:
            tracker_interface.update_instruments_in_metadata_db(reader, platform_name)
            json = tracker_interface.get_json_format_for_deployment(
                platform_name,
                start_time
            )
            pp.pprint(json)
            deployment_name = '{}-{}'.format(
                json['deployment']['glider'],
                json['deployment']['trajectory_date']
            )
            if deployment_name not in already_processed:
                already_processed[deployment_name] = []

        # import pprint
        # pp = pprint.PrettyPrinter(indent=4)
        # pp.pprint(sorted(list(group_headers(reader.headers).keys())))

        for i, pair in enumerate(sorted_files):
            if pair[0] in already_processed[deployment_name] or pair[1] in already_processed[deployment_name]:
                print("Already processed: %s. Skipping" % pair)
                continue
            attrs = json
            timestr = 'timestamp'

            if len(pair) < 2:
                continue

            flight_path = pair[0]
            science_path = pair[1]

            glider_name = attrs['deployment']['glider']

            try:
                try:
                    # Find profile breaks
                    profiles = find_profiles(flight_path, science_path, 'timestamp', 'm_depth-m')
                except IndexError:
                    print("Something strange happened on files: %s" % (pair))
                    print("Mission: %s" % deployment.__dict__)
                    continue
                    # raise

                # Interpolate GPS
                interp_gps = get_file_set_gps(
                    flight_path, science_path, timestr, 'm_gps_'
                )

                interp_time = get_file_set_timestamps(
                    flight_path,
                    science_path,
                    'm_present_time-timestamp',
                    'sci_m_present_time-timestamp'
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
                    fd, tmp_path = tempfile.mkstemp(dir=tmpdir, suffix='.nc', prefix='gutils')
                    os.close(fd)

                    # Open new NetCDF
                    begin_time = datetime.datetime.utcfromtimestamp(line[timestr])
                    filename = "%s_%s_%s.nc" % (
                        glider_name,
                        begin_time.strftime("%Y%m%dT%H%M%SZ"),
                        'realtime'
                    )

                    file_path = os.path.join(
                        nc_dir,
                        deployment_name,
                        filename
                    )

                    # NOTE: Store 1 based profile id
                    try:
                        init_netcdf(tmp_path, attrs, i + 1, profile_id + 1)
                    except:
                        print(tmp_path)
                        raise
                    profile = profiles[profiles[:, 2] == profile_id]
                    if len(profile) < 1:
                        continue
                    profile_end = max(profile[:, 0])

                if os.path.isfile(file_path):
                    # We already processed this file, carry on
                    print("Already created: %s. Skipping" % file_path)
                    break

                with sensor_tracker_interface.OpenGliderNetCDFWriterInterface(tmp_path, platform_name, start_time, 'a') as glider_nc:
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

            already_processed[deployment_name] += pair
            with open(processing_cache_file, 'wb') as f:
                pickle.dump(already_processed, f)


if __name__ == '__main__':
    main()
