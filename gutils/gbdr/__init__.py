#!/usr/bin/env python

from gutils.gbdr.methods import (
    create_glider_BD_ASCII_reader,
    find_glider_BD_headers,
    map_line
)


class GliderBDReader(object):
    """Reads a specified type or set of glider data files.

    Starts a process to read through the data from a set of glider
    binary data files.  Provides an iterator to process a line of
    data at a time.

    Arguments:
    path - Directory where binary data is stored.
    fileType - Types of files to process
    fileNames - An optional set specifying exactly which files to process.
    """

    def __init__(self, filePaths: list):
        self.reader = (
            create_glider_BD_ASCII_reader(filePaths)
        )

        self.headers = find_glider_BD_headers(self.reader)
        self.finished = False

    def __iter__(self):
        return self

    def __next__(self):
        if self.finished:
            raise StopIteration

        try:
            value = map_line(self.reader, self.headers)
            return value
        except EOFError:
            self.finished = True
            raise StopIteration


class MergedGliderBDReader(object):
    """Merges flight and science data readers to return merged glider data.

    Takes two glider binary data readers: one for
    flight and one for science data.  Provides an
    iterator that returns merged data from these readers
    as a dictionary.

    Arguments:
    flight_reader - A GliderBDReader object tied to flight data *bd files.
    science_reader - A GliderBDReader object tied to science data *bd files.
    merge_tolerance - An optional tolerance number of seconds to consider two
        rows mergeable.  Default: 1
    """

    def __init__(self, flight_reader: GliderBDReader, science_reader: GliderBDReader, merge_tolerance: int=1):
        self.flight_reader = flight_reader
        self.science_reader = science_reader
        self.merge_tolerance = merge_tolerance

        self.flight_headers = flight_reader.headers
        self.science_headers = science_reader.headers
        self.headers = self.flight_headers + self.science_headers

        self.__read_both_values()

    def __read_both_values(self):
        self.__read_flight_values()
        self.__read_science_values()

    def __read_flight_values(self):
        try:
            self.flight_values = self.flight_reader.__next__()
        except:
            self.flight_values = None

    def __read_science_values(self):
        try:
            self.science_values = self.science_reader.__next__()
        except StopIteration:
            self.science_values = None

    def __iter__(self):
        return self

    def __next__(self):
        if self.flight_values is not None or self.science_values is not None:
            ret_val = {}

            # No more flight values. Spit out the rest of the science values.
            if self.flight_values is None:
                ret_val = self.science_values
                self.__read_science_values()
            # No more science values. Spit out the rest of the flight values.
            elif self.science_values is None:
                ret_val = self.flight_values
                self.__read_flight_values()
            # We have both.  Time to merge.
            else:
                # To merge or not to merge
                flight_time = (
                    self.flight_values['timestamp']
                )
                science_time = (
                    self.science_values['timestamp']
                )
                time_diff = abs(flight_time - science_time)

                # Can merge because within tolerance
                if time_diff <= self.merge_tolerance:
                    ret_val = self.flight_values
                    ret_val.update(self.science_values)
                    self.__read_both_values()

                # Flight is ahead of science.  Return and get next science.
                elif flight_time > science_time:
                    ret_val = self.science_values
                    self.__read_science_values()

                # Science is ahead of flight.  Return and get next flight.
                else:
                    ret_val = self.flight_values
                    self.__read_flight_values()

            return ret_val
        else:
            raise StopIteration
