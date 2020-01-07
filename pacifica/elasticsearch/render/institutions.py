#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Search transaction rendering methods."""
from six import text_type
from peewee import JOIN
from pacifica.metadata.orm import Institutions, InstitutionUser, Transactions, Users, TransSIP, TransSAP
from .users import UsersRender
from .base import SearchBase, query_select_default_args


class InstitutionsRender(SearchBase):
    """Render an institution for search."""

    fields = [
        'obj_id', 'display_name', 'keyword', 'release',
        'updated_date', 'created_date'
    ]

    @classmethod
    @query_select_default_args
    # pylint: disable=too-many-arguments
    def get_select_query(cls, time_delta, obj_cls, time_field, page, enable_paging, items_per_page):
        """Generate the select query for groups related to instruments."""
        # pylint: disable=invalid-name
        SIPTrans = Transactions.alias()
        SAPTrans = Transactions.alias()
        # pylint: enable=invalid-name
        # pylint: disable=protected-access
        query = (
            Institutions.select()
            .join(InstitutionUser, JOIN.LEFT_OUTER, on=(InstitutionUser.institution == Institutions.id))
            .join(Users, JOIN.LEFT_OUTER, on=(InstitutionUser.user == Users.id))
            .join(TransSIP, JOIN.LEFT_OUTER, on=(TransSIP.submitter == Users.id))
            .join(SIPTrans, JOIN.LEFT_OUTER, on=(SIPTrans.id == TransSIP.id))
            .join(TransSAP, JOIN.LEFT_OUTER, on=(TransSAP.submitter == Users.id))
            .join(SAPTrans, JOIN.LEFT_OUTER, on=(SAPTrans.id == TransSAP.id))
            .where(
                (getattr(Institutions, time_field) > time_delta) |
                (getattr(Users, time_field) > time_delta) |
                (getattr(InstitutionUser, time_field) > time_delta) |
                (getattr(TransSIP, time_field) > time_delta) |
                (getattr(TransSAP, time_field) > time_delta) |
                (getattr(SIPTrans, time_field) > time_delta) |
                (getattr(SAPTrans, time_field) > time_delta))
            .order_by(Institutions.id)
            .distinct()
        )
        # pylint: enable=protected-access
        if enable_paging:
            return query.paginate(page, items_per_page)
        return query

    @staticmethod
    def obj_id(**inst_obj):
        """Return string for object id."""
        return text_type('institutions_{_id}').format(**inst_obj)

    @staticmethod
    def updated_date(**inst_obj):
        """Return string for the updated date."""
        return text_type('{updated}').format(**inst_obj)

    @staticmethod
    def created_date(**inst_obj):
        """Return string for the created date."""
        return text_type('{created}').format(**inst_obj)

    @staticmethod
    def display_name(**inst_obj):
        """Return the string to render display_name."""
        return text_type('{name}').format(**inst_obj)

    @staticmethod
    def keyword(**inst_obj):
        """Return the rendered string for keywords."""
        return text_type('{name}').format(**inst_obj)

    @classmethod
    def release(cls, **_inst_obj):
        """Return whether the institution has released anything."""
        return 'true'

    @classmethod
    def get_transactions(cls, **inst_obj):
        """Return the list of transaction ids for the institution."""
        ret = set()
        for inst_user_obj in cls.get_rel_by_args('institution_user', institution=inst_obj['_id']):
            ret.update(UsersRender.get_transactions(_id=inst_user_obj['user']))
        return [
            'transactions_{}'.format(trans_id)
            for trans_id in ret
        ]
