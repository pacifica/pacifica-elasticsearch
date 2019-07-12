#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Global static variables."""
from os import getenv
from os.path import expanduser, join

CONFIG_FILE = getenv(
    'ELASTICSEARCH_CONFIG',
    join(expanduser('~'), '.pacifica-elasticsearch', 'config.ini')
)
CHERRYPY_CONFIG = getenv(
    'ELASTICSEARCH_CPCONFIG',
    join(expanduser('~'), '.pacifica-elasticsearch', 'cpconfig.ini')
)
