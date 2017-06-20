# Glider Utilities (GUTILS)

[![Build Status](https://travis-ci.org/SECOORA/GUTILS.svg?branch=master)](https://travis-ci.org/SECOORA/GUTILS)

A set of Python utilities for reading, merging, and post processing Teledyne Webb Slocum Glider data.


## Installation

Available through [`conda`](http://conda.pydata.org/docs/install/quick.html). This library was designed with Python 3.5 or above in mind. I backported it to 2.7 by request. Please use 3.5+ if possible as the [gwt project](https://github.com/TEOS-10/GSW-Python) will soon end support for 2.7.

```
$ conda create -n sgs python=3.5
$ source activate sgs
$ conda install -c axiom-data-science gutils
```


## Configuration

Example configuration files from the University of South Florida Glider Group can be found in the [test resources](https://github.com/axiom-data-science/GUTILS/tree/master/tests/resources/usf-bass).  Use these files as a basis for your institution, possible datatypes, and gliders.  *Do not delete any parameters from these files, only adjust their values for your institution.  Otherwise, your NetCDF files will not pass the check_glider_netcdf.py script.*

A brief overview of each file and folder follows:

* [your-glider-name-here/global_attributes.json](https://github.com/axiom-data-science/GUTILS/blob/master/tests/resources/usf-bass/global_attributes.json) contains parameters that are specific to the glider institution.
* [your-glider-name-here/deployment.json](https://github.com/axiom-data-science/GUTILS/blob/master/tests/resources/usf-bass/deployment.json) describes the current deployment for a given glider.  Includes global attribute details that change between deployments and information about the glider/platform deployed.
* [your-glider-name-here/instruments.json](https://github.com/axiom-data-science/GUTILS/blob/master/tests/resources/usf-bass/instruments.json) provides details about instruments deployed with a single glider.  instrument_ctd is the only required instrument in this file.
* [your-glider-name-here/datatypes.json](https://github.com/axiom-data-science/GUTILS/blob/master/tests/resources/usf-bass/datatypes.json) maps between glider generated types (e.g., m_depth-m) and types to be output to a NetCDF file (e.g., depth). *This file is optional and will fallback to the defaults if not present in the configuration directory*. You will only need to edit this file if you need to add datatypes to the NetCDF files.  *Types in here that are not produced by your glider will NOT cause errors.*  Hopefully, through collaboration, we will be able to produce a complete mapping of glider types to NetCDF variables and keep the default updated. Please submit a PR if you are using a custom `datatypes.json`!

## Basic Usage

#### CLI

###### Create NetCDF File

```bash
$ create_glider_netcdf.py -h
```

```bash
$ create_glider_netcdf.py \
    -f <path to flight file> \
    -s <path to science file> \
    <glider config directory> \
    <NetCDF output directory>
```

Outputs a set of profiles from a merged flight and science dataset NetCDF files to the output directory.  *Can also specify only a flight (-f) or science (-s) file without the corresponding file.*

For the example above, the glider config directory would be something like [this](https://github.com/axiom-data-science/GUTILS/tree/master/tests/resources/usf-bass).  Point directly at a glider configuration directory.

###### Check NetCDF File

```bash
$ check_glider_netcdf.py -h
```

```bash
$ check_glider_netcdf.py <path to NetCDF file>
```

Prints errors and returns number of errors.  Prints PASS and returns 0 on success.


#### Python

```python
from gutils.gbdr import (
    GliderBDReader,
    MergedGliderBDReader
)
from gutils.nc import open_glider_netcdf

flightReader = GliderBDReader(
    ['/some/path/to/file.sbd']
)
scienceReader = GliderBDReader(
    ['/some/path/to/file.tbd']
)
reader = MergedGliderBDReader(flightReader, scienceReader)

with open_glider_netcdf(self.test_path, self.mode) as glider_nc:
    for line in reader:
        glider_nc.stream_dict_insert(line)
```

See a larger example in [tests.py](https://github.com/axiom-data-science/GUTILS/blob/master/tests/test_nc.py)



# SECOORA Glider System (SGS)

This package is part of the SECOORA Glider System (SGS) and was originally developed by the [CMS Ocean Technology Group](http://www.marine.usf.edu/COT/) at the University of South Florida. It is now maintained by [SECOORA](http://secoora.org) and [Axiom Data Science](http://axiomdatascience.com).

* [GUTILS](https://github.com/axiom-data-science/GUTILS): A set of Python utilities for post processing glider data.
* [GSPS](https://github.com/axiom-data-science/GSPS): Watches a directory for new *db flight/science files and publishes the data to a ZeroMQ socket.
* [GDAM](https://github.com/axiom-data-science/GDAM): Watches a directory for new *db flight/science files, inserts the data into a MongoDB instance, and publishes the data to a ZeroMQ socket.
