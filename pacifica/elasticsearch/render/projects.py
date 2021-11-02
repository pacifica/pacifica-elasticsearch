#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Search transaction rendering methods."""
from six import text_type
from peewee import JOIN
from pacifica.metadata.orm import Projects, Users, ProjectUser, Relationships, Institutions, InstitutionUser
from pacifica.metadata.orm import Instruments, ProjectInstrument, Groups, InstrumentGroup, TransSIP, TransSAP
from .base import SearchBase, query_select_default_args
from .users import UsersRender
from .institutions import InstitutionsRender
from .instruments import InstrumentsRender
from .groups import GroupsRender
from .science_themes import ScienceThemesRender


class ProjectsRender(SearchBase):
    """Render a project for search."""

    fields = [
        'obj_id', 'display_name', 'abstract', 'title',
        'keyword', 'release', 'closed_date', 'actual_end_date',
        'updated_date', 'created_date', 'actual_start_date'
    ]

    rel_objs = [
        'users', 'institutions', 'instruments', 'groups',
        'released_count', 'science_themes'
    ]

    @classmethod
    def get_render_query(cls,obj_cls,id):
        return (
            Projects.select()
            .join(ProjectUser, JOIN.LEFT_OUTER, on=(ProjectUser.project == Projects.id))
            .join(Users, JOIN.LEFT_OUTER, on=(ProjectUser.user == Users.id))
            .join(Relationships, JOIN.LEFT_OUTER, on=(Relationships.uuid == ProjectUser.relationship))
            .join(InstitutionUser, JOIN.LEFT_OUTER, on=(Users.id == InstitutionUser.user))
            .join(Institutions, JOIN.LEFT_OUTER, on=(InstitutionUser.institution == Institutions.id))
            .join(ProjectInstrument, JOIN.LEFT_OUTER, on=(ProjectInstrument.project == Projects.id))
            .join(Instruments, JOIN.LEFT_OUTER, on=(ProjectInstrument.instrument == Instruments.id))
            .join(InstrumentGroup, JOIN.LEFT_OUTER, on=(InstrumentGroup.instrument == Instruments.id))
            .join(Groups, JOIN.LEFT_OUTER, on=(InstrumentGroup.group == Groups.id))
            .join(TransSIP, JOIN.LEFT_OUTER, on=(TransSIP.project == Projects.id))
            .join(TransSAP, JOIN.LEFT_OUTER, on=(TransSAP.project == Projects.id))
            .where(Projects.id == id)
        )

    @classmethod
    @query_select_default_args
    def get_select_query(cls, time_delta, obj_cls, time_field):
        """Return the select query based on kwargs provided."""
        return (
            Projects.select()
            .join(ProjectUser, JOIN.LEFT_OUTER, on=(ProjectUser.project == Projects.id))
            .join(Users, JOIN.LEFT_OUTER, on=(ProjectUser.user == Users.id))
            .join(Relationships, JOIN.LEFT_OUTER, on=(Relationships.uuid == ProjectUser.relationship))
            .join(InstitutionUser, JOIN.LEFT_OUTER, on=(Users.id == InstitutionUser.user))
            .join(Institutions, JOIN.LEFT_OUTER, on=(InstitutionUser.institution == Institutions.id))
            .join(ProjectInstrument, JOIN.LEFT_OUTER, on=(ProjectInstrument.project == Projects.id))
            .join(Instruments, JOIN.LEFT_OUTER, on=(ProjectInstrument.instrument == Instruments.id))
            .join(InstrumentGroup, JOIN.LEFT_OUTER, on=(InstrumentGroup.instrument == Instruments.id))
            .join(Groups, JOIN.LEFT_OUTER, on=(InstrumentGroup.group == Groups.id))
            .join(TransSIP, JOIN.LEFT_OUTER, on=(TransSIP.project == Projects.id))
            .join(TransSAP, JOIN.LEFT_OUTER, on=(TransSAP.project == Projects.id))
            .where(
                (getattr(Projects, time_field) > time_delta) |
                (getattr(ProjectUser, time_field) > time_delta) |
                (getattr(Relationships, time_field) > time_delta) |
                (getattr(InstitutionUser, time_field) > time_delta) |
                (getattr(Institutions, time_field) > time_delta) |
                (getattr(TransSIP, time_field) > time_delta) |
                (getattr(TransSAP, time_field) > time_delta) |
                (getattr(Instruments, time_field) > time_delta) |
                (getattr(InstrumentGroup, time_field) > time_delta) |
                (getattr(ProjectInstrument, time_field) > time_delta) |
                (getattr(Groups, time_field) > time_delta))
            .order_by(Projects.id)
            .distinct()
        )

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
    def display_name(**proj_obj):
        """Return the string to render display_name."""
        return text_type('{title}').format(**proj_obj)

    @staticmethod
    def created_date(**proj_obj):
        """Return string for the created date."""
        return text_type('{created}').format(**proj_obj)

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
    def _transaction_release(cls, trans_id):
        """Return 'true' if transaction has been release."""
        # pylint: disable=import-outside-toplevel
        # pylint: disable=cyclic-import
        from .transactions import TransactionsRender
        return TransactionsRender.release(_id=trans_id)

    @classmethod
    def released_count_obj_lists(cls, **proj_obj):
        """Count the released transactions associated with this project."""
        ret = 0
        for trans_id in cls._transsip_transsap_merge({'project': proj_obj['_id']}, '_id'):
            if cls._transaction_release(trans_id) == 'true':
                ret += 1
        return ret

    @classmethod
    def get_transactions(cls, **proj_obj):
        """Return the list of transaction ids for the project."""
        return [
            'transactions_{}'.format(trans_id)
            for trans_id in cls._transsip_transsap_merge({'project': proj_obj['_id']}, '_id')
        ]

    @classmethod
    def institutions_obj_lists(cls, **proj_obj):
        """Get the institutions related to the transaction."""
        ret = set()
        for proj_user_obj in cls.get_rel_by_args('project_user', project=proj_obj['_id']):
            for inst_obj in cls.get_rel_by_args('institution_user', user=proj_user_obj['user']):
                ret.update([inst_obj['institution']])
        return [
            InstitutionsRender.render(
                cls.get_rel_by_args('institutions', _id=inst_id)[0]
            ) for inst_id in ret
        ]

    @classmethod
    def instruments_obj_lists(cls, **proj_obj):
        """Get the instruments related to the transaction."""
        return [
            InstrumentsRender.render(
                cls.get_rel_by_args('instruments', _id=proj_inst_obj['instrument'])[0]
            ) for proj_inst_obj in cls.get_rel_by_args('project_instrument', project=proj_obj['_id'])
        ]

    @classmethod
    def groups_obj_lists(cls, **proj_obj):
        """Get the instrument groups related to the project."""
        ret = set()
        for proj_inst_obj in cls.get_rel_by_args('project_instrument', project=proj_obj['_id']):
            for group_obj in cls.get_rel_by_args('instrument_group', instrument=proj_inst_obj['instrument']):
                ret.update([group_obj['group']])
        return [
            GroupsRender.render(
                cls.get_rel_by_args('groups', _id=group_id)[0], True
            ) for group_id in ret
        ]

    @classmethod
    def science_themes_obj_lists(cls, **proj_obj):
        """Get science themes related to the transaction."""
        return [
            ScienceThemesRender.render(proj_obj)
        ]

    @classmethod
    def users_obj_lists(cls, **proj_obj):
        """Get the user objects and relationships."""
        ret = {}
        for proj_user_obj in cls.get_rel_by_args('project_user', project=proj_obj['_id']):
            rel_obj = cls.get_rel_by_args('relationships', uuid=proj_user_obj['relationship'])[0]
            rel_list = ret.get(rel_obj['name'], [])
            try:
                rel_list.append(
                    UsersRender.render(cls.get_rel_by_args('users', _id=proj_user_obj['user'])[0])
                )
            except IndexError:
                print("IndexError getting user for project ", proj_user_obj)
            ret[rel_obj['name']] = rel_list
        return ret
