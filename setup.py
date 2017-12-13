#!/usr/bin/env python

from setuptools import setup

setup(
    name = 'schizophrenia'
    ,version = '0.2.6'
    ,author = 'frank2'
    ,author_email = 'frank2@dc949.org'
    ,description = 'A task-based threading library based on Python threads.'
    ,license = 'GPLv3'
    ,keywords = 'threading'
    ,url = 'https://github.com/frank2/schizophrenia'
    ,package_dir = {'schizophrenia': 'schizophrenia'}
    ,packages = ['schizophrenia']
    ,install_requires = []
    ,python_requires = '>=3'
    ,classifiers = [
        'Development Status :: 4 - Beta'
        ,'Topic :: Software Development :: Libraries'
        ,'License :: OSI Approved :: GNU General Public License v3 (GPLv3)']
)
