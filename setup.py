#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Setup and install the pacifica service."""
from os import path
from setuptools import setup, find_packages


setup(
    name='pacifica-elasticsearch',
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    description='Pacifica Elasticsearch Library',
    url='https://github.com/pacifica/pacifica-elasticsearch/',
    long_description=open(path.join(
        path.abspath(path.dirname(__file__)),
        'README.md')).read(),
    long_description_content_type='text/markdown',
    author='David Brown',
    author_email='dmlb2000@gmail.com',
    packages=find_packages(include=['pacifica.*']),
    namespace_packages=['pacifica'],
    install_requires=[
        'celery',
        'elasticsearch',
        'pacifica-metadata>=0.11.0,<1',
        'pacifica-namespace',
        'python-dateutil',
        'tqdm',
        'memoization'
    ],
    include_package_data=True,
    package_data={'': ['*.json']},
    entry_points={
        'console_scripts': [
            'pacifica-elasticsearch=pacifica.elasticsearch.__main__:main',
        ],
    },
)
