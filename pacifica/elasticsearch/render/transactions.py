#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Search transaction rendering methods."""
from six import text_type
from peewee import DoesNotExist, JOIN
from pacifica.metadata.orm import DOITransaction, TransactionUser, Transactions, Users, Instruments, TransSIP
from pacifica.metadata.orm import TransSAP, Groups, InstrumentGroup, Relationships, Files, Projects, Keys, Values
from pacifica.metadata.orm import TransactionKeyValue
from ..config import get_config
from .users import UsersRender
from .instruments import InstrumentsRender
from .groups import GroupsRender
from .projects import ProjectsRender
from .keys import KeysRender
from .values import ValuesRender
from .files import FilesRender
from .base import SearchBase


class TransactionsRender(SearchBase):
    """Render a transaction for search."""

    fields = [
        'obj_id', 'access_url', 'has_doi', 'release',
        'updated_date', 'created_date', 'description'
    ]
    rel_objs = [
        'users', 'instruments', 'groups', 'projects', 'key_value_pairs', 'files'
    ]

    @classmethod
    def get_select_query(cls, time_delta, obj_cls, **kwargs):
        """Return the select query based on kwargs provided."""
        time_field = kwargs.get('time_field', 'updated')
        page = kwargs.get('page', 0)
        items_per_page = kwargs.get('items_per_page', 20)
        enable_paging = kwargs.get('enable_paging', True)
        # The alias() method does return a class
        # pylint: disable=invalid-name
        ReleaseUsers = Users.alias()
        SIPUsers = Users.alias()
        SAPUsers = Users.alias()
        SIPProjects = Projects.alias()
        SAPProjects = Projects.alias()
        # pylint: enable=invalid-name
        # pylint: disable=protected-access
        query = (
            Transactions.select()
            .join(TransactionUser, JOIN.LEFT_OUTER, on=(TransactionUser.transaction == Transactions.id))
            .join(DOITransaction, JOIN.LEFT_OUTER, on=(DOITransaction.transaction == TransactionUser.uuid))
            .join(Relationships, JOIN.LEFT_OUTER, on=(Relationships.uuid == TransactionUser.relationship))
            .join(ReleaseUsers, JOIN.LEFT_OUTER, on=(TransactionUser.user == ReleaseUsers.id))
            .join(TransSIP, JOIN.LEFT_OUTER, on=(TransSIP.id == Transactions.id))
            .join(TransSAP, JOIN.LEFT_OUTER, on=(TransSAP.id == Transactions.id))
            .join(SIPUsers, JOIN.LEFT_OUTER, on=(TransSIP.submitter == SIPUsers.id))
            .join(SAPUsers, JOIN.LEFT_OUTER, on=(TransSAP.submitter == SAPUsers.id))
            .join(Instruments, JOIN.LEFT_OUTER, on=(Instruments.id == TransSIP.instrument))
            .join(SIPProjects, JOIN.LEFT_OUTER, on=(SIPProjects.id == TransSIP.project))
            .join(SAPProjects, JOIN.LEFT_OUTER, on=(SAPProjects.id == TransSAP.project))
            .join(InstrumentGroup, JOIN.LEFT_OUTER, on=(InstrumentGroup.instrument == TransSIP.instrument))
            .join(Groups, JOIN.LEFT_OUTER, on=(Groups.id == InstrumentGroup.group))
            .join(Files, JOIN.LEFT_OUTER, on=(Files.transaction == Transactions.id))
            .join(TransactionKeyValue, JOIN.LEFT_OUTER, on=(TransactionKeyValue.transaction == Transactions.id))
            .join(Keys, JOIN.LEFT_OUTER, on=(TransactionKeyValue.key == Keys.id))
            .join(Values, JOIN.LEFT_OUTER, on=(TransactionKeyValue.value == Values.id))
            .where(
                (getattr(Files, time_field) > time_delta) |
                (getattr(DOITransaction, time_field) > time_delta) |
                (getattr(Transactions, time_field) > time_delta) |
                (getattr(TransactionUser, time_field) > time_delta) |
                (getattr(Relationships, time_field) > time_delta) |
                (getattr(ReleaseUsers, time_field) > time_delta) |
                (getattr(SIPUsers, time_field) > time_delta) |
                (getattr(SAPUsers, time_field) > time_delta) |
                (getattr(SAPProjects, time_field) > time_delta) |
                (getattr(SIPProjects, time_field) > time_delta) |
                (getattr(TransSIP, time_field) > time_delta) |
                (getattr(TransSAP, time_field) > time_delta) |
                (getattr(Instruments, time_field) > time_delta) |
                (getattr(InstrumentGroup, time_field) > time_delta) |
                (getattr(Groups, time_field) > time_delta) |
                (getattr(TransactionKeyValue, time_field) > time_delta) |
                (getattr(Keys, time_field) > time_delta) |
                (getattr(Values, time_field) > time_delta))
            .order_by(obj_cls._meta.primary_key)
            .distinct()
        )
        # pylint: enable=protected-access
        if enable_paging:
            return query.paginate(page, items_per_page)
        return query

    @staticmethod
    def obj_id(**trans_obj):
        """Return string for object id."""
        return text_type('transactions_{_id}').format(**trans_obj)

    @staticmethod
    def description(**trans_obj):
        """Return string for the updated date."""
        return text_type('{description}').format(**trans_obj)

    @staticmethod
    def updated_date(**trans_obj):
        """Return string for the updated date."""
        return text_type('{updated}').format(**trans_obj)

    @staticmethod
    def created_date(**trans_obj):
        """Return string for the created date."""
        return text_type('{created}').format(**trans_obj)

    @classmethod
    def release(cls, **trans_obj):
        """Return 'true' if transaction has been release."""
        predicate = ((TransactionUser.transaction == trans_obj['_id']) & (
            TransactionUser.relationship == cls.releaser_uuid))
        query = (TransactionUser.select().where(predicate))
        try:
            query.get()
            ret = 'true'
        except DoesNotExist:
            ret = 'false'
        return ret

    @classmethod
    def access_url(cls, **trans_obj):
        """Figure out the access url for the transaction."""
        trans_doi = cls.get_trans_doi(trans_obj['_id'])
        if trans_doi != 'false':
            trans_obj['doi'] = trans_doi
            return get_config().get('policy', 'doi_url_format').format(**trans_obj)
        if cls.release(**trans_obj) == 'true':
            return get_config().get('policy', 'release_url_format').format(**trans_obj)
        return get_config().get('policy', 'internal_url_format').format(**trans_obj)

    @classmethod
    def get_trans_doi(cls, trans_id):
        """Get the transaction doi or return false."""
        preciate = (TransactionUser.uuid == DOITransaction.transaction)
        where_clause = ((TransactionUser.relationship == cls.releaser_uuid) & (TransactionUser.transaction == trans_id))
        # pylint: disable=no-member
        query = (DOITransaction.select().join(TransactionUser, on=preciate).where(where_clause))
        # pylint: enable=no-member
        try:
            ret = query.get().doi
        except DoesNotExist:
            ret = 'false'
        return ret

    @classmethod
    def has_doi(cls, **trans_obj):
        """Return boolean if the transaction has a DOI."""
        if cls.get_trans_doi(trans_obj['_id']) != 'false':
            return 'true'
        return 'false'

    @classmethod
    def files_obj_lists(cls, **trans_obj):
        """Get the projects related to the transaction."""
        return [
            FilesRender.render(file_obj)
            for file_obj in cls.get_rel_by_args('files', transaction=trans_obj['_id'])
        ]

    @classmethod
    def users_obj_lists(cls, **trans_obj):
        """Get the user objects and relationships."""
        ret = {'submitter': []}
        for user_id in cls._transsip_transsap_merge({'_id': trans_obj['_id']}, 'submitter'):
            ret['submitter'].append(
                UsersRender.render(cls.get_rel_by_args('users', _id=user_id)[0])
            )
        for trans_user_obj in cls.get_rel_by_args('transaction_user', transaction=trans_obj['_id']):
            rel_obj = cls.get_rel_by_args('relationships', uuid=trans_user_obj['relationship'])[0]
            rel_list = ret.get(rel_obj['name'], [])
            rel_list.append(
                UsersRender.render(cls.get_rel_by_args('users', _id=trans_user_obj['user'])[0])
            )
            ret[rel_obj['name']] = rel_list
        return ret

    @classmethod
    def instruments_obj_lists(cls, **trans_obj):
        """Get the instruments related to the transaction."""
        ret = set()
        for trans_obj in cls.get_rel_by_args('transsip', _id=trans_obj['_id']):
            ret.update([trans_obj['instrument']])
        return [
            InstrumentsRender.render(
                cls.get_rel_by_args('instruments', _id=inst_id)[0]
            ) for inst_id in ret
        ]

    @classmethod
    def groups_obj_lists(cls, **trans_obj):
        """Get the instrument groups related to the transaction."""
        ret = set()
        for inst_obj in cls.get_rel_by_args('transsip', _id=trans_obj['_id']):
            for group_obj in cls.get_rel_by_args('instrument_group', instrument=inst_obj['instrument']):
                ret.update([group_obj['group']])
        return [
            GroupsRender.render(
                cls.get_rel_by_args('groups', _id=group_id)[0]
            ) for group_id in ret
        ]

    @classmethod
    def projects_obj_lists(cls, **trans_obj):
        """Get the projects related to the transaction."""
        return [
            ProjectsRender.render(
                cls.get_rel_by_args('projects', _id=proj_id)[0]
            ) for proj_id in cls._transsip_transsap_merge({'_id': trans_obj['_id']}, 'project')
        ]

    @classmethod
    def key_value_pairs_obj_lists(cls, **trans_obj):
        """Get the key value pairs related to the transaction."""
        ret = {
            'key_value_hash': {},
            'key_objs': [],
            'value_objs': []
        }
        for trans_kvp_obj in cls.get_rel_by_args('trans_key_value', transaction=trans_obj['_id']):
            key_obj = cls.get_rel_by_args('keys', _id=trans_kvp_obj['key'])[0]
            value_obj = cls.get_rel_by_args('values', _id=trans_kvp_obj['value'])[0]
            ret['key_objs'].append(KeysRender.render(key_obj))
            ret['value_objs'].append(ValuesRender.render(value_obj))
            ret['key_value_hash'][key_obj['key']] = value_obj['value']
        return ret

    # this doesn't get executed but is needed to be here to satisfy abstract method
    @classmethod
    def get_transactions(cls, **_trans_obj):  # pragma: no cover
        """Just return an empty list."""
        return []
