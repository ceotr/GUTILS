#!/usr/bin/python
# TODO: write tests
from gutils.gbdr import (
    GliderBDReader,
    MergedGliderBDReader
)

import datetime

import os
import glob

import sensor_tracker_interface


import numpy as np
import netCDF4 as nc4

from gutils.nc import open_glider_netcdf
from gutils.scripts.create_glider_netcdf import process_dataset


if __name__ == '__main__':
    raw_data_path = '/home/bcovey/full_res_data/subset/'
    platform = 'otn200'

    flight_files = sorted(glob.glob(os.path.join(raw_data_path, '*.DBD')))
    flightReader = GliderBDReader(flight_files)

    science_files = sorted(glob.glob(os.path.join(raw_data_path, '*.EBD')))
    scienceReader = GliderBDReader(science_files)

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



    nc_path = 'test.nc'

    with open_glider_netcdf(nc_path, 'w') as glider_nc:

        # Set global attributes
        glider_nc.set_global_attributes(json['global'])

        # Set Trajectory
        glider_nc.set_trajectory_id(
            json['deployment']['glider'],
            json['deployment']['trajectory_date']
        )

        traj_str = "{}-{}".format(
            json['deployment']['glider'],
            json['deployment']['trajectory_date']
        )

        # Set Platform
        glider_nc.set_platform(json['deployment']['platform'])

        # Set Instruments
        glider_nc.set_instruments(json['instruments'])

        # Set Segment ID
        glider_nc.set_segment_id(3)

        # Set Profile ID
        glider_nc.set_profile_id(4)

        for line in reader:
            glider_nc.stream_dict_insert(line)

        glider_nc.update_profile_vars()
        glider_nc.calculate_salinity()
        glider_nc.calculate_density()
        glider_nc.update_bounds()
