#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Search transaction rendering methods."""
from six import text_type
from .base import SearchBase


class RelationshipsRender(SearchBase):
    """Render an relationship for search."""

    fields = [
        'obj_id', 'name', 'display_name', 'keyword',
        'updated_date', 'created_date'
    ]

    @classmethod
    def get_index_query(cls,obj_cls,**kwargs):
        """Generate the select query to give all the rows of class"""
        return (obj_cls.select(obj_cls.uuid))

    @classmethod
    def get_render_query(cls,obj_cls,id):
        return (obj_cls.select().where(obj_cls.uuid == id))

    @staticmethod
    def obj_id(**rel_obj):
        """Return string for object id."""
        return text_type('relationships_{uuid}').format(**rel_obj)

    @staticmethod
    def updated_date(**rel_obj):
        """Return string for the updated date."""
        return text_type('{updated}').format(**rel_obj)

    @staticmethod
    def created_date(**rel_obj):
        """Return string for the created date."""
        return text_type('{created}').format(**rel_obj)

    @staticmethod
    def display_name(**rel_obj):
        """Return the string to render display_name."""
        return text_type('{display_name}').format(**rel_obj)

    @staticmethod
    def keyword(**rel_obj):
        """Return the rendered string for keywords."""
        return text_type('{display_name}').format(**rel_obj)

    @classmethod
    def name(cls, **rel_obj):
        """Return the relationship name."""
        return text_type('{name}').format(**rel_obj)

    @classmethod
    def get_transactions(cls, **_kwargs):
        """Just return an empty list."""
        return []
