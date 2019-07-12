#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Celery work queue interface."""
from __future__ import absolute_import


class CeleryQueue:
    """Class to implement the queue interface."""

    def __init__(self):
        """Default constructor."""
        self.by_obj_type = {}
        self.all_jobs = []

    def put(self, job_dict):
        """Save job dictionary and run job."""
        from .tasks import work_on_job
        result = work_on_job.delay(job_dict)
        self.all_jobs.append((job_dict, result))
        if not job_dict['object'] in self.by_obj_type:
            self.by_obj_type[job_dict['object']] = []
        self.by_obj_type[job_dict['object']].append((job_dict, result))
