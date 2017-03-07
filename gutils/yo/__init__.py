#!/usr/bin/env python

import numpy as np

from gutils import (
    validate_glider_args,
    clean_dataset,
    boxcar_smooth_dataset
)

# For Readability
TIME_DIM = 0
DATA_DIM = 1


def binarize_diff(data):
    data[data <= 0] = -1
    data[data >= 0] = 1
    return data


def calculate_delta_depth(interp_data):
    delta_depth = np.diff(interp_data)
    delta_depth = binarize_diff(delta_depth)

    delta_depth = boxcar_smooth_dataset(delta_depth, 2)

    return delta_depth


def create_profile_entry(dataset, start, end):
    time_start = dataset[start, TIME_DIM]
    time_end = dataset[end - 1, TIME_DIM]
    depth_start = dataset[start, DATA_DIM]
    depth_end = dataset[end - 1, DATA_DIM]
    return {
        'index_bounds': (start, end),
        'time_bounds': (time_start, time_end),
        'depth_bounds': (depth_start, depth_end)
    }


def find_yo_extrema(timestamps, depth):
    """Returns timestamps, row indices, and depth and time bounds
    corresponding to glider yo profiles

    Returns the timestamps and row indices corresponding the peaks and valleys
    (profiles start/stop) found in the time-depth array, tz.  All indices are
    returned.

    Parameters:
        time, depth

    Returns:
        A Nx3 array of timestamps, depth, and profile ids

    Use filter_yo_extrema to remove invalid/incomplete profiles
    """

    validate_glider_args(timestamps, depth)

    est_data = np.column_stack((
        timestamps,
        depth
    ))

    # Set negative depth values to NaN
    est_data[est_data[:, DATA_DIM] <= 0] = float('nan')

    est_data = clean_dataset(est_data)

    if len(est_data) > 0:
        # Stretch estimated values for interpolation to span entire dataset
        interp_data = np.interp(
            timestamps,
            est_data[:, 0],
            est_data[:, 1],
            left=est_data[0, 1],
            right=est_data[-1, 1]
        )

        interp_data = boxcar_smooth_dataset(interp_data, 5)

        delta_depth = calculate_delta_depth(interp_data)

        interp_indices = np.argwhere(delta_depth == 0).flatten()

        profiled_dataset = np.zeros((len(timestamps), 3))
        profiled_dataset[:, TIME_DIM] = timestamps
        profiled_dataset[:, DATA_DIM] = interp_data
    else:
        # If there are no cleaned depth and time values
        # This seems to happen if the glider spends a segment at the surface
        profiled_dataset = np.zeros((len(timestamps), 3))
        profiled_dataset[:, TIME_DIM] = timestamps
        profiled_dataset[:, DATA_DIM] = depth
        interp_indices = []

    start_index = 0
    for profile_id, end_index in enumerate(interp_indices):
        profiled_dataset[start_index:end_index, 2] = profile_id

        start_index = end_index

    if start_index < len(profiled_dataset):
        profiled_dataset[start_index:, 2] = len(interp_indices) - 1

    return profiled_dataset
