#!/usr/bin/python
# -*- coding: utf-8 -*-
"""This is the admin main method."""
from __future__ import print_function, absolute_import
import logging
from sys import argv as sys_argv
from argparse import ArgumentParser
from datetime import timedelta
from .search_sync import search_sync

logging.basicConfig()
LOGGER = logging.getLogger('peewee')


def objstr_to_timedelta(obj_str):
    """Turn an object string of the format X unit ago into timedelta."""
    value, unit, check = obj_str.split()
    assert check == 'after' or check == 'ago'
    return timedelta(**{unit: float(value)})


def exclude_options(obj_str):
    """Turn an object string into expressive exclude option."""
    obj_cls_attr, value = obj_str.split('=')
    obj_cls, obj_attr = obj_cls_attr.split('.')
    return (obj_cls, obj_attr, value)


def searchsync_options(searchsync_parser):
    """Add the searchsync command line options."""
    searchsync_parser.add_argument(
        '--exclude', dest='exclude',
        help='object and attr to exclude (i.e. --exclude="projects._id=1234").',
        nargs='*', default=set(), type=exclude_options
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
    args.func(args)
