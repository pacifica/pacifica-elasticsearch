#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Search transaction rendering methods."""
from six import text_type
from peewee import JOIN
from pacifica.metadata.orm import Users, TransSIP, TransSAP
from .base import SearchBase, query_select_default_args


class UsersRender(SearchBase):
    """Render a user for search."""

    fields = [
        'obj_id', 'display_name', 'keyword', 'release',
        'updated_date', 'created_date'
    ]

    @classmethod
    @query_select_default_args
    # pylint: disable=arguments-differ,too-many-arguments
    def get_select_query(cls, time_delta, obj_cls, time_field, page, enable_paging, items_per_page):
        """Return the select query based on kwargs provided."""
        # pylint: disable=protected-access
        query = (
            Users.select()
            .join(TransSIP, JOIN.LEFT_OUTER, on=(TransSIP.submitter == Users.id))
            .join(TransSAP, JOIN.LEFT_OUTER, on=(TransSAP.submitter == Users.id))
            .where(
                (getattr(Users, time_field) > time_delta) |
                (getattr(TransSIP, time_field) > time_delta) |
                (getattr(TransSAP, time_field) > time_delta))
            .order_by(Users.id)
            .distinct()
        )
        # pylint: enable=protected-access
        if enable_paging:
            return query.paginate(page, items_per_page)
        return query

    @staticmethod
    def obj_id(**user_obj):
        """Return string for object id."""
        return text_type('users_{_id}').format(**user_obj)

    @staticmethod
    def updated_date(**user_obj):
        """Return string for the updated date."""
        return text_type('{updated}').format(**user_obj)

    @staticmethod
    def created_date(**user_obj):
        """Return string for the created date."""
        return text_type('{created}').format(**user_obj)

    @staticmethod
    def display_name(**user_obj):
        """Return the string to render display_name."""
        return text_type('{last_name}, {first_name} {middle_initial}').format(**user_obj)

    @staticmethod
    def keyword(**user_obj):
        """Return the rendered string for keywords."""
        return text_type('{last_name}, {first_name} {middle_initial}').format(**user_obj)

    @classmethod
    def release(cls, **user_obj):
        """Return whether the user has released anything."""
        for trans_id in cls._transsip_transsap_merge({'submitter': user_obj['_id']}, '_id'):
            if cls.get_rel_by_args('transaction_user', transaction=trans_id, relationship=cls.releaser_uuid):
                return 'true'
        return 'false'

    @classmethod
    def get_transactions(cls, **user_obj):
        """Return the list of transaction ids for the user."""
        return [
            'transactions_{}'.format(trans_id)
            for trans_id in cls._transsip_transsap_merge({'submitter': user_obj['_id']}, '_id')
        ]
