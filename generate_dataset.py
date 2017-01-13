#!/usr/bin/python

from gutils.gbdr import (
    GliderBDReader,
    MergedGliderBDReader
)

flightReader = GliderBDReader(
    ['./tests/resources/usf-bass/usf-bass-2014-061-1-0.sbd']
)
scienceReader = GliderBDReader(
    ['./tests/resources/usf-bass/usf-bass-2014-061-1-0.tbd']
)
reader = MergedGliderBDReader(flightReader, scienceReader)

with open('./tests/resources/usf-bass/ctd_dataset.csv', 'w') as f:
    for line in reader:
        f.write('%f,%f,%f,%f,%f,%f\r\n' % (
            line.get('timestamp'),
            line.get('sci_water_cond-s/m', float('nan')),
            line.get('sci_water_temp-degc', float('nan')),
            line.get('sci_water_pressure-bar', float('nan')),
            line.get('m_lat-lat', float('nan')),
            line.get('m_lon-lon', float('nan'))
        ))
