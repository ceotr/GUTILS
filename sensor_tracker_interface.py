import re
import os
import datetime

from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine, MetaData, or_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import case, func
import sqlalchemy
import atexit

from netCDF4 import Dataset

import database_config

from gutils.nc import GliderNetCDFWriter

special_instruments = [
    'm',
    'sci_m',
    'c'
]


def add_or_append(dictionary, key, value):
    if key not in dictionary:
        dictionary[key] = [value]
    else:
        if value not in dictionary[key]:
            dictionary[key].append(value)


def group_headers(headers):
    instruments = {}
    for header in headers:
        match = re.match(r'^(sci_[a-zA-Z0-9]*)_(.*)', header['name'])
        if match:
            add_or_append(instruments, match.group(1), '%s-%s' % (header['name'], header['units'].lower()))
        else:
            match = re.match(r'^([a-zA-Z]*)_(.*)', header['name'])
            if match:
                add_or_append(instruments, match.group(1), '%s-%s' % (header['name'], header['units'].lower()))
            else:
                raise Exception("Encountered an unidentifiable instrument: %s" % header['name'])
    return instruments


def replace_none(val):
    if val is None or val == '':
        return ' '
    return val


class SensorTrackerInterface(object):
    def __init__(self):
        engine = create_engine(database_config.DATABASE_CONNECTOR)
        m = MetaData()
        m.reflect(engine)
        self.Base = automap_base(metadata=m)
        self.Base.prepare(engine, reflect=True)
        self.Session = sessionmaker(bind=engine)
        self.session = self.Session()

    def __enter__(self):
        return self

    def get_instrument(self, instrument_identifier, platform_name, time):
        # TODO: make this send emails if there are overlapping instruments or if instrument is not in db
        Platform = self.Base.classes.platforms_platform
        Instrument_on_platform = self.Base.classes.instruments_instrumentonplatform
        Instrument = self.Base.classes.instruments_instrument

        result = self.session.query(Instrument).join(
            Instrument_on_platform,
            Platform
        ).filter(
            Instrument.identifier == instrument_identifier,
            Instrument_on_platform.start_time <= time,
            or_(
                Instrument_on_platform.end_time >= time,
                Instrument_on_platform.end_time.is_(None)
            ),
            Platform.name == platform_name
        ).one()
        return result

    def get_sensors(self, instrument_id):
        Instrument = self.Base.classes.instruments_instrument
        Sensor = self.Base.classes.instruments_sensor

        result = self.session.query(Sensor).join(
            Instrument
        ).filter(
            Instrument.id == instrument_id
        ).all()
        return result

    def get_platform(self, platform_name):
        Platform = self.Base.classes.platforms_platform

        result = self.session.query(Platform).filter(
            Platform.name == platform_name
        ).one()
        return result

    def get_platform_deployment(self, platform_name, start_time):
        Platform = self.Base.classes.platforms_platform
        PlatformDeployment = self.Base.classes.platforms_platformdeployment

        result = self.session.query(PlatformDeployment).join(Platform).filter(
            Platform.name == platform_name,
            PlatformDeployment.start_time == start_time.strftime('%Y-%m-%d %H:%M:%S')
        ).one()
        return result

    def get_deployments(self, platform_name=None, deployment_number=None):
        Platform = self.Base.classes.platforms_platform
        PlatformDeployment = self.Base.classes.platforms_platformdeployment

        if platform_name is not None:
            result = self.session.query(PlatformDeployment, Platform).join(Platform).filter(
                Platform.name == platform_name
            )
        else:
            result = self.session.query(PlatformDeployment, Platform).join(Platform)

        if deployment_number is not None:
            result = result.filter(PlatformDeployment.deployment_number == deployment_number)

        return result.all()

    def get_active_deployments(self):
        Platform = self.Base.classes.platforms_platform
        PlatformDeployment = self.Base.classes.platforms_platformdeployment
        PlatformType = self.Base.classes.platforms_platformtype


        result = self.session.query(
            PlatformDeployment,
            Platform
        ).join(
            Platform
        ).join(PlatformType).filter(
            PlatformDeployment.end_time == None,
            or_(
                PlatformType.model == 'Slocum Glider G2',
                PlatformType.model == 'Slocum Glider G1'
            )
        )

        return result.all()

    def get_deployment_institution(self, platform_name, start_time):
        Platform = self.Base.classes.platforms_platform
        PlatformDeployment = self.Base.classes.platforms_platformdeployment
        Institution = self.Base.classes.general_institution

        result = self.session.query(Institution).join(Platform, PlatformDeployment).filter(
            Platform.name == platform_name,
            PlatformDeployment.start_time == start_time.strftime('%Y-%m-%d %H:%M:%S')
        ).one()
        return result

    def get_deployment_project(self, platform_name, start_time):
        Platform = self.Base.classes.platforms_platform
        PlatformDeployment = self.Base.classes.platforms_platformdeployment
        Project = self.Base.classes.general_project

        result = self.session.query(Project).join(PlatformDeployment, Platform).filter(
            Platform.name == platform_name,
            PlatformDeployment.start_time == start_time.strftime('%Y-%m-%d %H:%M:%S')
        ).one()
        return result

    def get_deployment_instruments(self, platform_name, start_time):
        Instrument = self.Base.classes.instruments_instrument
        InstrumentOnPlatform = self.Base.classes.instruments_instrumentonplatform
        Platform = self.Base.classes.platforms_platform

        deployment = self.get_platform_deployment(platform_name, start_time)
        # Use nested SQL case statements to get either the row with the most recent end_time
        # or no end time
        # This is pretty ugly, here's some doc:
        # - http://stackoverflow.com/questions/21286215/how-can-i-include-null-values-in-a-min-or-max
        # - http://stackoverflow.com/questions/11258770/case-when-with-orm-sqlalchemy
        most_recent = case(
            [(func.max(case(
                [(InstrumentOnPlatform.end_time.is_(None), 1), ],
                else_=0
            )) == 0, func.max(InstrumentOnPlatform.end_time))]
        )
        if deployment.end_time is not None:
            result = self.session.query(
                Instrument,
                most_recent
            ).join(InstrumentOnPlatform, Platform).filter(
                Platform.name == platform_name,
                InstrumentOnPlatform.start_time <= deployment.start_time,
                or_(
                    InstrumentOnPlatform.end_time >= deployment.end_time,
                    InstrumentOnPlatform.end_time.is_(None)
                )
            ).group_by(Instrument.id).all()
        else:
            result = self.session.query(
                Instrument,
                most_recent
            ).join(InstrumentOnPlatform, Platform).filter(
                Platform.name == platform_name,
                InstrumentOnPlatform.start_time <= deployment.start_time
            ).group_by(Instrument.id).all()
        results = []
        for i in result:
            results.append(i.instruments_instrument)
        return results

    def get_output_sensors(self, instrument_id):
        Instrument = self.Base.classes.instruments_instrument
        Sensor = self.Base.classes.instruments_sensor

        results = self.session.query(Sensor).join(Instrument).filter(
            Instrument.id==instrument_id,
            Sensor.include_in_output==True
        ).all()
        return results

    def get_json_format_for_deployment(self, platform_name, start_time):
        attrs = {}

        # build a dict of global attributes
        attrs['global'] = {
            "featureType": "trajectory",
            "Conventions": "CF-1.6",
            "Metadata_Conventions": "CF-1.6, Unidata Dataset Discovery v1.0",
            "cdm_data_type": "Trajectory",
            "format_version": "IOOS_Glider_NetCDF_v2.0.nc",
            "keywords": "Oceans > Ocean Pressure > Water Pressure, Oceans > Ocean Temperature > Water Temperature, Oceans > Salinity/Density > Conductivity, Oceans > Salinity/Density > Density, Oceans > Salinity/Density > Salinity",
            "keywords_vocabulary": "GCMD Science Keywords",
            "license": "This data may be redistributed and used without restriction.",
            "metadata_link": " ",
            "processing_level": "Dataset taken from glider native file format and is provided as is with no expressed or implied assurance of quality assurance or quality control.",
            "references": " ",
            "standard_name_vocabulary": "CF Standard Name Table v27",
            "source": "Observational data from a profiling glider",
            "title": "Slocum Glider Dataset",
            "platform_type": "Slocum Glider",
            "summary": "Gliders are small, free-swimming, unmanned vehicles that use changes in buoyancy to move vertically and horizontally through the water column in a saw-tooth pattern. They are deployed for days to several months and gather detailed information about the physical, chemical and biological processes of the world\"s The Slocum glider was designed and oceans. built by Teledyne Webb Research Corporation, Falmouth, MA, USA.",
            "comment": ""
        }

        # get the deployment record
        try:
            deployment = self.get_platform_deployment(platform_name, start_time)
        except sqlalchemy.orm.exc.NoResultFound:
            raise ValueError('No deployment found for specified platform and start time: %s - %s' % (platform_name, start_time))

        # get the deployment institution
        try:
            institution = self.get_deployment_institution(platform_name, start_time)
        except sqlalchemy.orm.exc.NoResultFound:
            raise ValueError('No institution found for specified deployment: %s - %s' % (platform_name, start_time))
        institution_url = institution.url.split('.')
        institution_url.reverse()
        naming_authority = '.'.join(institution_url)

        # get the deployment project
        try:
            project = self.get_deployment_project(platform_name, start_time)
        except sqlalchemy.orm.exc.NoResultFound:
            raise ValueError('No project found for specified deployment: %s - %s' % (platform_name, start_time))

        # get the platform
        try:
            platform = self.get_platform(platform_name)
        except sqlalchemy.orm.exc.NoResultFound:
            raise ValueError("No platform named: %s found in sensor_tracker" % platform_name)

        # build a dict of deployment specific attributes
        attrs['deployment'] = {}
        attrs['deployment']['glider'] = platform_name
        attrs['deployment']['trajectory_date'] = deployment.start_time.strftime('%Y%m%dT%H%MZ')

        attrs['global'].update({
            "wmo_id": replace_none(deployment.wmo_id),
            "naming_authority": replace_none(naming_authority),
            "institution": replace_none(institution.name),
            "creator_email": replace_none(deployment.creator_email),
            "creator_name": replace_none(deployment.creator_name),
            "creator_url": replace_none(deployment.creator_url),
            "publisher_email": replace_none(deployment.publisher_email),
            "publisher_name": replace_none(deployment.publisher_name),
            "publisher_url": replace_none(deployment.publisher_url),
            "contributor_name": replace_none(deployment.contributor_name),
            "contributor_role": replace_none(deployment.contributor_role),
            "metadata_link": replace_none(deployment.metadata_link),
            "ioos_regional_association": " ",
            "acknowledgement": " ",
            "project": replace_none(project.name),
            "sea_name": replace_none(deployment.sea_name),
            "title": replace_none(deployment.title),
            "comment": replace_none(deployment.comment),
            "references": replace_none(deployment.references)
        })

        attrs['deployment']['platform'] = {
            "type": "platform",
            "id": replace_none(platform.wmo_id),
            "wmo_id": replace_none(platform.wmo_id),
            "long_name": replace_none(platform.name),
            "comment": " ",
            "instrument": " ",
            "type": "platform"
        }

        attrs['instruments'] = []
        instruments = self.get_deployment_instruments(platform_name, start_time)
        for inst in instruments:
            if inst.identifier not in special_instruments:
                attrs['instruments'].append({
                    'name': 'instrument_%s' % (inst.short_name.replace(' ', '_')),
                    'type': 'i4',
                    'attrs': {
                        'serial_number': replace_none(inst.serial),
                        'make_model': replace_none(inst.long_name),
                        'comment': replace_none(inst.comment),
                        'long_name': replace_none(inst.long_name),
                        'calibration_date': ' ',
                        'user_calibrated': ' ',
                        'factory_calibrated': ' ',
                        'type': 'instrument',
                        'platform': 'platform'
                    }
                })
        return attrs

    def get_json_instrument_metadata_for_deployment(self, platform_name, start_time):
        instruments = self.get_deployment_instruments(platform_name, start_time)
        sensor_json = {}
        for i in instruments:
            sensors = self.get_output_sensors(i.id)
            for s in sensors:
                if s.long_name is None or s.long_name.strip() == '':
                    quality_flag = ' '
                else:
                    quality_flag = "%s Quality Flag" % s.long_name
                sensor_json[s.identifier] = {
                    "name": replace_none(s.long_name),
                    "type": s.type,
                    "dimension": "time",
                    "attrs": {
                        "units": replace_none(s.units),
                        "standard_name": replace_none(s.standard_name),
                        "valid_min": replace_none(s.valid_min),
                        "valid_max": replace_none(s.valid_max),
                        "long_name": replace_none(s.long_name),
                        "observation_type": "measured",
                        "platform": "platform",
                        "instrument": 'instrument_%s' % (i.short_name.replace(' ', '_')),
                        "coordinates": "lon lat depth time",
                        "accuracy": replace_none(s.accuracy),
                        "precision": replace_none(s.precision),
                        "resolution": replace_none(s.resolution),
                        "sensor_name": " ",
                        "comment": replace_none(s.comment)
                    },
                    "status_flag": {
                        "attrs": {
                            "long_name": quality_flag
                        }
                    }
                }
        return sensor_json

    def insert_sensor(self, sensor, instrument):
        Sensor = self.Base.classes.instruments_sensor
        s = Sensor(
            instrument_id=instrument.id,
            identifier=sensor,
            include_in_output=False,
            type='f8'
        )
        self.session.add(s)
        self.session.commit()
        return s

    def update_instruments_in_metadata_db(self, reader, platform_name):
        Instrument = self.Base.classes.instruments_instrument
        Instrument_on_platform = self.Base.classes.instruments_instrumentonplatform
        Platform = self.Base.classes.platforms_platform

        try:
            # Get the platform row
            platform = self.get_platform(platform_name)
        except:
            raise Exception("ERROR: Glider: %s not included in metadata db" % platform_name)

        instruments = group_headers(reader.headers)

        time = datetime.datetime.fromtimestamp(reader.flight_values['timestamp'])
        print("TIME: %s" % time)

        for inst in instruments:
            inst_row = None
            try:
                # Get the instrument entry
                inst_row = self.get_instrument(inst, platform_name, time)
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
                platform = self.get_platform(platform_name)
                if short_name is not None:
                    inst_row = Instrument(
                        identifier=inst,
                        short_name=short_name,
                        long_name=long_name,
                        serial=platform.serial_number
                    )
                    self.session.add(inst_row)
                    self.session.commit()
                    self.session.add(Instrument_on_platform(
                        platform_id=platform.id,
                        instrument_id=inst_row.id,
                        start_time=platform.purchase_date
                    ))
                    self.session.commit()

            if inst_row is not None:
                # Get the sensors
                sensors = self.get_sensors(inst_row.id)
                # Build a list of identifiers in the database
                sensor_identifiers = [s.identifier for s in sensors]
                print("\nUpdating database for: %s" % inst)
                print("Sensors added to database:")
                # Find the ones that aren't already in the database
                for sensor in instruments[inst]:
                    if sensor not in sensor_identifiers:
                        sensors.append(self.insert_sensor(sensor, inst_row))
                        print("\t* %s" % sensor)
                print("Done updating %s" % inst)
            else:
                print("Instrument not being processed: %s" % inst)
        return

    def __exit__(self, exc_type, exc_value, traceback):
        self.Session.close_all()
        print("Bye!")


DEFAULT_GLIDER_BASE = os.path.dirname(__file__)


def open_glider_netcdf(output_path, platform, start_time, mode=None, COMP_LEVEL=None,
                       config_path=None, DEBUG=False):
    mode = mode or 'w'
    COMP_LEVEL = COMP_LEVEL or 1
    config_path = config_path or DEFAULT_GLIDER_BASE
    return OpenGliderNetCDFWriterInterface(output_path, platform, start_time, mode, COMP_LEVEL, config_path, DEBUG)


class OpenGliderNetCDFWriterInterface(GliderNetCDFWriter):
    def __init__(self, output_path, platform, start_time, mode=None, COMP_LEVEL=None,
                 config_path=None, DEBUG=False):
        self.nc = None
        self.output_path = output_path
        self.mode = mode or 'w'
        self.COMP_LEVEL = COMP_LEVEL or 1
        self.config_path = config_path or DEFAULT_GLIDER_BASE
        self.DEBUG = DEBUG
        self.datatypes = {}
        self.platform = platform
        self.start_time = start_time

    def __enter__(self):
        """ Opens the NetCDF file. Sets up QAQC and time variables.
        Updates global history variables.
        Called at beginning of Python with block.
        """
        self = super(OpenGliderNetCDFWriterInterface, self).__enter__()
        self.__append_datatypes()
        return self

    def __exit__(self, type, value, tb):
        # GliderNetCDFWriter.__exit__(self, type, value, tb)
        super(OpenGliderNetCDFWriterInterface, self).__exit__(type, value, tb)

    def __append_datatypes(self):
        """ Pulls datatypes from the sensor tracker database and merges with datatypes.json
        """
        platform = self.platform
        start_time = self.start_time

        # Append types from the sensor tracker
        st = SensorTrackerInterface()
        sensors = st.get_json_instrument_metadata_for_deployment(platform, start_time)
        for s in sensors:
            if s not in self.datatypes:
                self.datatypes[s] = sensors[s]
            else:
                self.datatypes[s]['attrs'].update(sensors[s]['attrs'])
