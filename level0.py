#!/usr/bin/python
# TODO: write tests
from gutils.gbdr import (
    GliderBDReader,
    MergedGliderBDReader
)

import os
import glob
import re
import datetime

from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine, MetaData, or_
from sqlalchemy.orm import sessionmaker
import sqlalchemy
import atexit
import database_config

engine = create_engine(database_config.DATABASE_CONNECTOR)
m = MetaData()
m.reflect(engine)
Base = automap_base(metadata=m)
Base.prepare(engine, reflect=True)
Session = sessionmaker(bind=engine)
session = Session()


def add_or_append(dictionary, key, value):
    if key not in dictionary:
        dictionary[key] = [value]
    else:
        dictionary[key].append(value)


def group_headers(headers):
    instruments = {}
    for header in headers:
        match = re.match(r'^(sci_[a-zA-Z0-9]*)_(.*)', header['name'])
        if match:
            add_or_append(instruments, match.group(1), header['name'])
        else:
            match = re.match(r'^([a-zA-Z]*)_(.*)', header['name'])
            if match:
                add_or_append(instruments, match.group(1), header['name'])
            else:
                raise Exception("Encountered an unidentifiable instrument: %s" % header['name'])
    return instruments


def get_instrument(instrument_identifier, platform_name, time):
    # TODO: make this send emails if there are overlapping instruments or if instrument is not in db
    Platform = Base.classes.platforms_platform
    Instrument_on_platform = Base.classes.instruments_instrumentonplatform
    Instrument = Base.classes.instruments_instrument

    result = session.query(Instrument).join(
        Instrument_on_platform,
        Platform
    ).filter(
        Instrument.identifier == instrument_identifier,
        Instrument_on_platform.start_time <= time,
        or_(
            Instrument_on_platform.end_time >= time,
            Instrument_on_platform.end_time == None
        ),
        Platform.name == platform_name
    ).one()
    return result


def get_sensors(instrument_id):
    Instrument = Base.classes.instruments_instrument
    Sensor = Base.classes.instruments_sensor

    result = session.query(Sensor).join(
        Instrument
    ).filter(
        Instrument.id == instrument_id
    ).all()
    return result


def get_platform(platform_name):
    Platform = Base.classes.platforms_platform

    result = session.query(Platform).filter(
        Platform.name == platform_name
    ).one()
    return result


def insert_sensor(sensor, instrument):
    Sensor = Base.classes.instruments_sensor
    s = Sensor(
        instrument_id=instrument.id,
        identifier=sensor,
        include_in_output=False
    )
    session.add(s)
    session.commit()
    return s


def update_instruments_in_metadata_db(reader, platform_name):
    Instrument = Base.classes.instruments_instrument
    Instrument_on_platform = Base.classes.instruments_instrumentonplatform
    Platform = Base.classes.platforms_platform

    try:
        # Get the platform row
        platform = session.query(Platform).filter(
            Platform.name == platform_name
        ).one()
    except:
        raise Exception("ERROR: Glider: %s not included in metadata db" % platform_name)

    instruments = group_headers(reader.headers)

    time = datetime.datetime.fromtimestamp(reader.flight_values['timestamp'])
    print("TIME: %s" % time)

    for inst in instruments:
        inst_row = None
        try:
            # Get the instrument entry
            inst_row = get_instrument(inst, platform_name, time)
        except sqlalchemy.orm.exc.NoResultFound:
            # If it's one of the 'special' slocum instruments, add it to the db
            short_name = None
            long_name = None
            if inst == 'c':
                short_name = 'flight commanded'
                long_name = 'flight computer commanded'
            elif inst == 'm':
                short_name = 'flight measured'
                long_name = 'flight computer measured'
            elif inst == 'sci_m':
                short_name = 'science measured'
                long_name = 'science computer measured'
            else:
                print("WARNING: instrument: %s not currently in metadata db, skipping." % inst)
            platform = get_platform(platform_name)
            if short_name is not None:
                inst_row = Instrument(
                    identifier=inst,
                    short_name=short_name,
                    long_name=long_name,
                    serial=platform.serial_number
                )
                session.add(inst_row)
                session.commit()
                session.add(Instrument_on_platform(
                    platform_id=platform.id,
                    instrument_id=inst_row.id,
                    start_time=platform.purchase_date
                ))
                session.commit()

        if inst_row is not None:
            # Get the sensors
            sensors = get_sensors(inst_row.id)
            # Build a list of identifiers in the database
            sensor_identifiers = [s.identifier for s in sensors]
            print("\nUpdating database for: %s" % inst)
            print("Sensors added to database:")
            # Find the ones that aren't already in the database
            for sensor in instruments[inst]:
                if sensor not in sensor_identifiers:
                    sensors.append(insert_sensor(sensor, inst_row))
                    print("\t* %s" % sensor)
            print("Done updating %s" % inst)
        else:
            print("Instrument not being processed: %s" % inst)
    return


def exit_handler():
    Session.close_all()
    print("Bye!")

atexit.register(exit_handler)


if __name__ == '__main__':
    raw_data_path = '/home/bcovey/full_res_data/subset/'
    platform = 'otn200'

    flight_files = sorted(glob.glob(os.path.join(raw_data_path, '*.DBD')))
    flightReader = GliderBDReader(flight_files)

    science_files = sorted(glob.glob(os.path.join(raw_data_path, '*.EBD')))
    scienceReader = GliderBDReader([science_files[0]])

    reader = MergedGliderBDReader(flightReader, scienceReader)

    import pprint
    pp = pprint.PrettyPrinter(indent=4)
    print("Instruments in data:")
    pp.pprint(sorted(list(group_headers(reader.headers).keys())))

    update_instruments_in_metadata_db(reader, platform)
    # import pprint
    # pp = pprint.PrettyPrinter(indent=4)
    # pp.pprint(sorted(list(group_headers(reader.headers).keys())))
