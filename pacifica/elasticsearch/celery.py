#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Celery work queue interface."""
from __future__ import absolute_import
from tqdm import trange


SYNC_OBJECTS = [
    'keys',
    'values',
    'relationships',
    'transactions',
    'projects',
    'users',
    'instruments',
    'institutions',
    'groups'
]


class CeleryQueue(object):
    """Class to implement the queue interface."""

    def __init__(self):
        """Default constructor."""
        self.by_obj_type = {}
        self.all_jobs = []

    def put(self, job_dict):
        """Save job dictionary and run job."""
        # pylint: disable=cyclic-import
        from .tasks import work_on_job
        # pylint: enable=cyclic-import
        result = work_on_job.delay(job_dict)
        self.all_jobs.append((job_dict, result))
        if not job_dict['object'] in self.by_obj_type:
            self.by_obj_type[job_dict['object']] = []
        self.by_obj_type[job_dict['object']].append((job_dict, result))

    def progress(self):
        """
        Display progress bars on all items working.

        This is going to do the naive thing for now, a better
        solution would be to dynamically walk all jobs finding
        complete ones and updating instead of blocking on a job
        waiting for it to complete.
        """
        success = True
        for object_index in trange(len(SYNC_OBJECTS), desc='Total Completed'):
            object_name = SYNC_OBJECTS[object_index]
            job_list = self.by_obj_type[object_name]
            for job_index in trange(len(job_list), desc='Total {} Completed'.format(object_name)):
                _job_dict, result = self.by_obj_type[object_name][job_index]
                if not result.get():  # pragma: no cover failure testing is hard
                    success = False
        return success
