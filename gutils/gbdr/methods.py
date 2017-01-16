#!/usr/bin/env python

import re
import io
import os
import shutil
import subprocess
from glob import glob


dbd2asc_path = shutil.which('dbd2asc')  # conda
if dbd2asc_path is None:
    dbd2asc_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bin', 'dbd2asc')  # pip


def parse_glider_filename(filename: str) -> dict:
    """
    Parses a glider filename and returns details in a dictionary

    Returns dictionary with the following keys:
    * 'glider': glider name
    * 'year': data file year created
    * 'day': data file julian date created
    * 'mission': data file mission id
    * 'segment': data file segment id
    * 'type': data file type
    """

    head, tail = os.path.split(filename)

    matches = re.search(r"([\w\d\-]+)-(\d+)-(\d+)-(\d+)-(\d+)\.(\w+)$", tail)

    if matches is not None:
        return {
            'path': head,
            'glider': matches.group(1),
            'year': int(matches.group(2)),
            'day': int(matches.group(3)),
            'mission': int(matches.group(4)),
            'segment': int(matches.group(5)),
            'type': matches.group(6)
        }
    else:
        raise ValueError(
            "Filename ({}) not in usual glider format: "
            "<glider name>-<year>-<julian day>-"
            "<mission>-<segment>.<extenstion>".format(filename)
        )


def generate_glider_filename(description: dict) -> str:
    """ Converts a glider data file details dictionary to filename """
    filename = (
        "{glider}-{year:d}-{day:03d}-{mission:d}-{segment}.{type}".format(**description)
    )
    return os.path.join(description['path'], filename)


def generate_stream(processArgs: list) -> io.StringIO:
    """ Runs a given process, outputs the resulting text as a StringIO """
    process = subprocess.run(processArgs, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return io.StringIO(process.stdout), process.returncode


def can_find_bd_index(path):
    # Iterate through previous segment files
    processArgs = [dbd2asc_path, '-c', '/tmp', path]
    returncode = 1
    file_details = parse_glider_filename(path)
    while returncode == 1 and file_details['segment'] >= 0:
        processArgs[3] = generate_glider_filename(file_details)
        stream, returncode = generate_stream(processArgs)
        file_details['segment'] -= 1

    # Report whether index found or not
    return returncode == 0


def process_file(path: str) -> io.StringIO:
    """Processes a single glider data file. Returns an io.StringIO

    Intelligently falls back if previous index file has not
    been processed for given data file.

    Raises and exception if data index cannot be found for
    given data file.
    """
    processArgs = [dbd2asc_path, '-c', '/tmp', path]
    stream, returncode = generate_stream(processArgs)

    # Fallback to find index bd file
    if returncode == 1:
        if can_find_bd_index(path):
            stream, returncode = generate_stream(processArgs)
        else:
            raise KeyError("Cannot find data file index for: %s" % path)

    return stream


def process_all_of_type(path: str, extension: str) -> io.StringIO:
    """Process glider data files of one type

    No fallback.  Assumed that operation big enough
    to avoid this issue.
    """
    processArgs = [dbd2asc_path, '-c', '/tmp']
    filesWildCard = '%s/*.%s' % (path, extension)
    processArgs.extend(glob(filesWildCard))
    stream, returncode = generate_stream(processArgs)
    return stream


def process_file_list(filePaths: list) -> io.StringIO:
    """Process a list of glider data files to ASCII.

    Intelligently falls back for each file if necessary.

    Raises KeyError exception if it cannot generate an index for a
    given file.

    Returns a io.StringIO
    """
    processArgs = [dbd2asc_path, '-c', '/tmp']

    for filePath in filePaths:
        processArgs.append(filePath)

    stream, returncode = generate_stream(processArgs)

    # Fallback in case the cache is not available
    if returncode == 1:
        for filePath in filePaths:
            if not can_find_bd_index(filePath):
                raise KeyError(
                    "Cannot find data file index for: {}".format(filePath)
                )

        # Reprocess the file list
        stream, returncode = generate_stream(processArgs)

    return stream


def create_glider_BD_ASCII_reader(filePaths: list) -> io.StringIO:
    """Creates a glider binary data reader over a set of files

    Returns an io.StringIO
    """
    return process_file_list(filePaths)


def find_glider_BD_headers(reader: io.StringIO) -> list:
    """Finds and returns available headers in a set of glider data files

    Arguments:
    reader - A raw glider binary data reader of type subprocess.Popen
    """

    # Bleed off extraneous headers
    # Stop when sci_m_present_time is found
    line = reader.readline()
    while len(line) > 0 and line.find('m_present_time') == -1:
        line = reader.readline()

    if line is '':
        raise EOFError('No headers found before end of file.')

    headersTemp = [ x for x in line.split(' ') if x.strip() ]

    unitsLine = reader.readline()
    if unitsLine is not None:
        unitsTemp = [ x for x in unitsLine.split(' ') if x.strip() ]
    else:
        raise EOFError('No units found before end of file.')

    headers = []
    for header, unit in zip(headersTemp, unitsTemp):
        if header and unit:
            description = {
                'name': header,
                'units': unit,
                'is_point': header.find('lat') != -1 or header.find('lon') != -1
            }
            headers.append(description)

    # Remove extraneous bytes line
    reader.readline()

    return headers


def get_decimal_degrees(lat_lon: str) -> float:
    """Converts glider gps coordinate ddmm.mmm to decimal degrees dd.ddd

    Arguments:
    lat_lon - A floating point latitude or longitude in the format ddmm.mmm
        where dd's are degrees and mm.mmm is decimal minutes.

    Returns decimal degrees float
    """

    if lat_lon == 0:
        return -1

    lat_lon_string = str(lat_lon)
    decimal_place = lat_lon_string.find('.')
    if decimal_place != -1:
        str_dec = lat_lon_string[0:decimal_place - 2]
        str_dec_fractional = lat_lon_string[decimal_place - 2:]
    elif abs(lat_lon) < 181:
        if(abs(lat_lon / 100) > 100):
            str_dec = lat_lon_string[0:3]
            str_dec_fractional = lat_lon_string[3:]
        else:
            str_dec = lat_lon_string[0:2]
            str_dec_fractional = lat_lon_string[2:]
    else:
        return -1

    try:
        dec = float(str_dec)
        dec_fractional = float(str_dec_fractional)
        if dec < 0:
            dec_fractional *= -1
        return dec + dec_fractional / 60
    except ValueError:
        return None


def map_line(reader: io.StringIO, headers: list) -> list:
    """Maps all non-NaN values in a glider data file to a known header

    Arguments:
    reader - A subprocess.Popen with headers discovered
    headers - Headers discovered in data file already
    """

    readings = {}

    line = reader.readline()

    if len(line) == 0:
        raise EOFError('That\'s all the data!')

    line = line.rstrip()

    value_strings = line.split(' ')
    for i, string in enumerate(value_strings):
        if string != 'NaN':
            value = float(string)

            if i < len(headers):
                if headers[i]['is_point']:
                    value = get_decimal_degrees(value)
                key = headers[i]['name'] + "-" + headers[i]['units']
                readings[key] = value

    # Provide generic timestamp regardless of type for iterator
    # convenience
    # Keep originals for those interested
    if 'm_present_time-timestamp' in readings:
        readings['timestamp'] = readings['m_present_time-timestamp']
    elif 'sci_m_present_time-timestamp' in readings:
        readings['timestamp'] = readings['sci_m_present_time-timestamp']

    return readings
