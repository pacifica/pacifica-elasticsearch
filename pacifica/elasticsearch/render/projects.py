#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Search transaction rendering methods."""
from six import text_type
from .base import SearchBase


class ProjectsRender(SearchBase):
    """Render a project for search."""

    fields = [
        'obj_id', 'display_name', 'abstract', 'title',
        'keyword', 'release', 'closed_date', 'actual_end_date',
        'updated_date', 'created_date', 'actual_start_date'
    ]

    @staticmethod
    def obj_id(**proj_obj):
        """Return string for object id."""
        return text_type('projects_{_id}').format(**proj_obj)

    @staticmethod
    def abstract(**proj_obj):
        """Return string for the updated date."""
        return text_type('{abstract}').format(**proj_obj)

    @staticmethod
    def title(**proj_obj):
        """Return string for the updated date."""
        return text_type('{title}').format(**proj_obj)

    @staticmethod
    def actual_end_date(**proj_obj):
        """Return string for the updated date."""
        if not proj_obj.get('actual_end_date'):
            return None
        return text_type('{actual_end_date}').format(**proj_obj)

    @staticmethod
    def actual_start_date(**proj_obj):
        """Return string for the updated date."""
        if not proj_obj.get('actual_start_date'):
            return None
        return text_type('{actual_start_date}').format(**proj_obj)

    @staticmethod
    def closed_date(**proj_obj):
        """Return string for the updated date."""
        if not proj_obj.get('closed_date'):
            return None
        return text_type('{closed_date}').format(**proj_obj)

    @staticmethod
    def updated_date(**proj_obj):
        """Return string for the updated date."""
        return text_type('{updated}').format(**proj_obj)

    @staticmethod
    def created_date(**proj_obj):
        """Return string for the created date."""
        return text_type('{created}').format(**proj_obj)

    @staticmethod
    def display_name(**proj_obj):
        """Return the string to render display_name."""
        return text_type('{title}').format(**proj_obj)

    @staticmethod
    def keyword(**proj_obj):
        """Return the rendered string for keywords."""
        return text_type('{title}').format(**proj_obj)

    @classmethod
    def release(cls, **proj_obj):
        """Return whether the project has released anything."""
        results = cls.get_rel_by_args('projects', _id=proj_obj['_id'])
        if results and results[0].get('accepted_date', False):
            return 'true'
        return 'false'

    @classmethod
    def get_transactions(cls, **proj_obj):
        """Return the list of transaction ids for the project."""
        return [
            'transactions_{}'.format(trans_id)
            for trans_id in cls._transsip_transsap_merge({'project': proj_obj['_id']}, '_id')
        ]
