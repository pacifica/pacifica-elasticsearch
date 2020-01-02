#!/usr/bin/python
# -*- coding: utf-8 -*-
"""This is the admin main method."""
from __future__ import print_function, absolute_import
import logging
from sys import argv as sys_argv
from argparse import ArgumentParser
from datetime import timedelta
from .celery import SYNC_OBJECTS
from .search_sync import search_sync

logging.basicConfig()
LOGGER = logging.getLogger('peewee')
DEFAULT_CMP_DATES = ['created', 'updated']


def objstr_to_timedelta(obj_str):
    """Turn an object string of the format X unit ago into timedelta."""
    value, unit, check = obj_str.split()
    assert check in ['after', 'ago']
    return timedelta(**{unit: float(value)})


def exclude_options(obj_str):
    """Turn an object string into expressive exclude option."""
    obj_cls_attr, value = obj_str.split('=')
    obj_cls, obj_attr = obj_cls_attr.split('.')
    return (obj_cls, obj_attr, value)


def object_options(obj_str):
    """Convert an object and validate type."""
    if obj_str not in SYNC_OBJECTS:
        raise ValueError('{} is not a valid search sync object'.format(obj_str))
    return obj_str


def cmp_date_options(date_str):
    """Validate the date string is something we can compare against."""
    if date_str not in DEFAULT_CMP_DATES:
        raise ValueError('{} is not in {}'.format(date_str, DEFAULT_CMP_DATES))
    return date_str


def searchsync_options(searchsync_parser):
    """Add the searchsync command line options."""
    searchsync_parser.add_argument(
        '--exclude', dest='exclude',
        help='object and attr to exclude (i.e. --exclude="projects._id=1234").',
        nargs='*', default=set(), type=exclude_options
    )
    searchsync_parser.add_argument(
        '--object', dest='objects',
        help='object to parse (i.e. --object="projects").',
        nargs='*', default=set(SYNC_OBJECTS), type=object_options
    )
    searchsync_parser.add_argument(
        '--compare-date', dest='compare_dates',
        help='date to compare objects to (i.e. --compare-date="created").',
        nargs='*', default=set(DEFAULT_CMP_DATES), type=cmp_date_options
    )
    searchsync_parser.add_argument(
        '--objects-per-page', default=40000,
        type=int, help='objects per bulk upload.',
        required=False, dest='items_per_page'
    )
    searchsync_parser.add_argument(
        '--threads', default=4, required=False,
        type=int, help='number of threads to sync data',
    )
    searchsync_parser.add_argument(
        '--time-ago', dest='time_ago', type=objstr_to_timedelta,
        help='only objects newer than X days ago (i.e. --time-ago="7 days ago").',
        required=False, default=timedelta(days=36500)
    )
    searchsync_parser.add_argument(
        '--celery', dest='celery', action='store_true',
        help='send work to celery queue instead of threads',
        required=False, default=False
    )
    searchsync_parser.set_defaults(func=search_sync)


def main(*argv):
    """Main method for admin command line tool."""
    parser = ArgumentParser()
    parser.add_argument(
        '--verbose', default=False, action='store_true',
        help='enable verbose debug output'
    )
    searchsync_options(parser)
    if not argv:  # pragma: no cover
        argv = sys_argv[1:]
    args = parser.parse_args(argv)
    if args.verbose:  # pragma: no cover this is for debugging
        LOGGER.setLevel('DEBUG')
    return args.func(args)
