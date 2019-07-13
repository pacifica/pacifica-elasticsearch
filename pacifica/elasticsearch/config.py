#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Configuration reading and validation module."""
from os import getenv
try:
    from ConfigParser import SafeConfigParser
except ImportError:  # pragma: no cover python 2 vs 3 issue
    from configparser import ConfigParser as SafeConfigParser
try:
    from functools import lru_cache
except ImportError:  # pragma: no cover python 2 vs 3 issue
    from backports.functools_lru_cache import lru_cache
from .globals import CONFIG_FILE


@lru_cache(maxsize=1)
def get_config():
    """
    Return the ConfigParser object with defaults set.

    Currently metadata API doesn't work with SQLite the queries are
    too complex and it only is supported with MySQL and PostgreSQL.
    """
    configparser = SafeConfigParser()
    configparser.add_section('policy')
    configparser.set(
        'policy', 'internal_url_format',
        getenv('INTERNAL_URL_FORMAT',
               'https://internal.example.com/{_id}')
    )
    configparser.set(
        'policy', 'release_url_format',
        getenv('RELEASE_URL_FORMAT',
               'https://release.example.com/{_id}')
    )
    configparser.set(
        'policy', 'doi_url_format',
        getenv('DOI_URL_FORMAT', 'https://dx.doi.org/{doi}')
    )
    configparser.add_section('celery')
    configparser.set('celery', 'broker_url', getenv(
        'BROKER_URL', 'pyamqp://'))
    configparser.set('celery', 'backend_url', getenv(
        'BACKEND_URL', 'rpc://'))
    configparser.add_section('elasticsearch')
    configparser.set('elasticsearch', 'cache_size', getenv(
        'CACHE_SIZE', '10000'))
    configparser.set('elasticsearch', 'url', getenv(
        'ELASTIC_ENDPOINT', 'http://127.0.0.1:9200'))
    configparser.set('elasticsearch', 'index', getenv(
        'ELASTIC_INDEX', 'pacifica_search'))
    configparser.set('elasticsearch', 'timeout', getenv(
        'ELASTIC_TIMEOUT', '60'))
    configparser.set('elasticsearch', 'sniff', getenv(
        'ELASTIC_ENABLE_SNIFF', 'True'))
    configparser.read(CONFIG_FILE)
    return configparser
