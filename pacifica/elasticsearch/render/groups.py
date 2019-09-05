#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Search transaction rendering methods."""
from six import text_type
from .base import SearchBase
from .instruments import InstrumentsRender


class GroupsRender(SearchBase):
    """Render a group for search."""

    fields = [
        'obj_id', 'display_name', 'keyword', 'release',
        'updated_date', 'created_date'
    ]

    rel_objs = [
        'instruments'
    ]

    @staticmethod
    def obj_id(**group_obj):
        """Return string for object id."""
        return text_type('groups_{_id}').format(**group_obj)

    @staticmethod
    def updated_date(**group_obj):
        """Return string for the updated date."""
        return text_type('{updated}').format(**group_obj)

    @staticmethod
    def created_date(**group_obj):
        """Return string for the created date."""
        return text_type('{created}').format(**group_obj)

    @staticmethod
    def display_name(**group_obj):
        """Return the string to render display_name."""
        return text_type('{display_name}').format(**group_obj)

    @staticmethod
    def keyword(**group_obj):
        """Return the rendered string for keywords."""
        return text_type('{name}').format(**group_obj)

    @classmethod
    def release(cls, **_group_obj):
        """Return whether the group has released anything."""
        return 'true'

    @classmethod
    def get_transactions(cls, **_group_obj):
        """Just return an empty list."""
        return []

    @classmethod
    def instruments_obj_lists(cls, **group_obj):
        """Get the instruments related to the group."""
        return [
            InstrumentsRender.render(
                cls.get_rel_by_args('instruments', _id=inst_group_obj['instrument'])[0]
            ) for inst_group_obj in cls.get_rel_by_args('instrument_group', group=group_obj['_id'])
        ]
