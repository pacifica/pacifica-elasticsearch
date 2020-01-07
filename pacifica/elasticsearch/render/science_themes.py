#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Search transaction rendering methods."""
from six import text_type
from peewee import JOIN
from pacifica.metadata.orm import Projects, TransSIP, TransSAP
from .base import SearchBase, query_select_default_args


class ScienceThemesRender(SearchBase):
    """Render an science theme for search."""

    fields = [
        'obj_id', 'display_name', 'keyword',
        'updated_date', 'created_date', 'release'
    ]

    @classmethod
    @query_select_default_args
    # pylint: disable=arguments-differ,too-many-arguments
    def get_select_query(cls, time_delta, obj_cls, time_field, page, enable_paging, items_per_page):
        """Return the select query based on kwargs provided."""
        # pylint: disable=protected-access
        query = (
            Projects.select()
            .join(TransSIP, JOIN.LEFT_OUTER, on=(TransSIP.project == Projects.id))
            .join(TransSAP, JOIN.LEFT_OUTER, on=(TransSAP.project == Projects.id))
            .where(
                (getattr(Projects, time_field) > time_delta) |
                (getattr(TransSIP, time_field) > time_delta) |
                (getattr(TransSAP, time_field) > time_delta))
            .order_by(Projects.id)
            .distinct()
        )
        # pylint: enable=protected-access
        if enable_paging:
            return query.paginate(page, items_per_page)
        return query

    @staticmethod
    def obj_id(**proj_obj):
        """Return string for object id."""
        return text_type('science_themes_{science_theme}').format(**proj_obj)

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
        return text_type('{science_theme}').format(**proj_obj)

    @staticmethod
    def keyword(**proj_obj):
        """Return the rendered string for keywords."""
        return text_type('{science_theme}').format(**proj_obj)

    @classmethod
    def release(cls, **_proj_obj):
        """Return whether the user has released anything."""
        return 'true'

    @classmethod
    def get_transactions(cls, **proj_obj):
        """Return the list of transaction ids for the science theme."""
        ret = set()
        for rel_proj_obj in cls.get_rel_by_args('projects', science_theme=proj_obj['science_theme']):
            ret.update([
                'transactions_{}'.format(trans_id)
                for trans_id in cls._transsip_transsap_merge({'project': rel_proj_obj['_id']}, '_id')
            ])
        return list(ret)
