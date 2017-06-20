#!/usr/bin/env python

from setuptools import setup


def readme():
    with open('README.md') as f:
        return f.read()


def version():
    with open('VERSION') as f:
        return f.read().strip()


reqs = [line.strip() for line in open('requirements.txt') if not line.startswith('#')]


setup(
    name='gutils',
    version=version(),
    description='A set of Python utilities for reading, merging, and post '
                'processing Teledyne Webb Slocum Glider data.',
    long_description=readme(),
    author='Kyle Wilcox',
    author_email='kyle@axiomdatascience.com',
    install_requires=reqs,
    url='https://github.com/axiom-data-science/GUTILS',
    packages=[
        'gutils',
        'gutils.yo',
        'gutils.gbdr'
    ],
    scripts=[
        'gutils/scripts/check_glider_netcdf.py',
        'gutils/scripts/create_glider_netcdf.py',
    ],
    include_package_data=True,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering'
    ],
)
