#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Celery tasks for processing work to elasticsearch."""
from __future__ import absolute_import
from celery import Celery
from .config import get_config
from .search_sync import es_client, try_doing_work

ES_CLI = es_client()
ES_APP = Celery(
    'elasticsearch',
    broker=get_config().get('celery', 'broker_url'),
    backend=get_config().get('celery', 'backend_url')
)


# Coverage doesn't seem to be catching this.
@ES_APP.task()
def work_on_job(job):  # pragma: no cover
    """Work on a job."""
    return try_doing_work(ES_CLI, job)
