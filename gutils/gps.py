#!/usr/bin/env python

from gutils import (
    validate_glider_args,
    clean_dataset
)

import numpy as np


def filter_locations(locations):
    locations[locations > 600000] = np.nan
    return locations


def interpolate_gps(timestamps, latitude, longitude):
    """Calculates interpolated GPS coordinates between the two surfacings
    in a single glider binary data file.

    Parameters:
        'dataset': An N by 3 numpy array of time, lat, lon pairs

    Returns interpolated gps dataset over entire time domain of dataset
    """

    latitude = filter_locations(latitude)
    longitude = filter_locations(longitude)

    dataset = np.column_stack((
        timestamps,
        latitude,
        longitude
    ))

    dataset = clean_dataset(dataset)

    est_lat = np.zeros(len(latitude))
    est_lon = np.zeros(len(longitude))

    # If only one GPS point, make it the same for the entire dataset
    if len(dataset) == 1:
        est_lat[:] = dataset[0, 1]
        est_lon[:] = dataset[0, 2]
    elif len(dataset) == 0:
        est_lat[:] = np.nan
        est_lon[:] = np.nan
    else:
        # Interpolate data
        est_lat = np.interp(
            timestamps,
            dataset[:, 0],
            dataset[:, 1],
            left=dataset[0, 1],
            right=dataset[-1, 1]
        )
        est_lon = np.interp(
            timestamps,
            dataset[:, 0],
            dataset[:, 2],
            left=dataset[0, 2],
            right=dataset[-1, 2]
        )

    return est_lat, est_lon
