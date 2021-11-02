#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Search transaction rendering methods."""
from six import text_type
from peewee import JOIN
from pacifica.metadata.orm import Instruments, TransSIP, Keys, Values, InstrumentKeyValue
from .keys import KeysRender
from .values import ValuesRender
from .base import SearchBase, query_select_default_args


class InstrumentsRender(SearchBase):
    """Render instruments for search."""

    fields = [
        'obj_id', 'display_name', 'keyword', 'release',
        'updated_date', 'created_date'
    ]
    rel_objs = ['key_value_pairs']

    @classmethod
    def get_render_query(cls,obj_cls,id):
        return (
            Instruments.select()
            .join(InstrumentKeyValue, JOIN.LEFT_OUTER, on=(InstrumentKeyValue.instrument == Instruments.id))
            .join(Keys, JOIN.LEFT_OUTER, on=(InstrumentKeyValue.key == Keys.id))
            .join(Values, JOIN.LEFT_OUTER, on=(InstrumentKeyValue.key == Values.id))
            .join(TransSIP, JOIN.LEFT_OUTER, on=(TransSIP.instrument == Instruments.id))
            .where(Instruments.id == id)
        )

    @classmethod
    @query_select_default_args
    def get_select_query(cls, time_delta, obj_cls, time_field):
        """Generate the select query for groups related to instruments."""
        return (
            Instruments.select()
            .join(InstrumentKeyValue, JOIN.LEFT_OUTER, on=(InstrumentKeyValue.instrument == Instruments.id))
            .join(Keys, JOIN.LEFT_OUTER, on=(InstrumentKeyValue.key == Keys.id))
            .join(Values, JOIN.LEFT_OUTER, on=(InstrumentKeyValue.key == Values.id))
            .join(TransSIP, JOIN.LEFT_OUTER, on=(TransSIP.instrument == Instruments.id))
            .where(
                (getattr(Instruments, time_field) > time_delta) |
                (getattr(InstrumentKeyValue, time_field) > time_delta) |
                (getattr(TransSIP, time_field) > time_delta) |
                (getattr(Values, time_field) > time_delta) |
                (getattr(Keys, time_field) > time_delta))
            .order_by(Instruments.id)
            .distinct()
        )

    @staticmethod
    def obj_id(**instrument_obj):
        """Return string for object id."""
        return text_type('instruments_{_id}').format(**instrument_obj)

    @staticmethod
    def updated_date(**instrument_obj):
        """Return string for the updated date."""
        return text_type('{updated}').format(**instrument_obj)

    @staticmethod
    def created_date(**instrument_obj):
        """Return string for the created date."""
        return text_type('{created}').format(**instrument_obj)

    @staticmethod
    def display_name(**instrument_obj):
        """Return the string to render display_name."""
        return text_type('{display_name}').format(**instrument_obj)

    @staticmethod
    def keyword(**instrument_obj):
        """Return the rendered string for keywords."""
        return text_type('{display_name}').format(**instrument_obj)

    @classmethod
    def release(cls, **_instrument_obj):
        """Return whether the user has released anything."""
        return 'true'

    @classmethod
    def key_value_pairs_obj_lists(cls, **inst_obj):
        """Get the key value pairs related to the instruments."""
        ret = {
            'key_value_hash': {},
            'key_objs': [],
            'value_objs': []
        }
        inst_kvp_objs = cls.get_rel_by_args(
            'instrument_key_value',
            instrument=inst_obj['_id'],
            relationship=cls.search_required_uuid
        )
        for inst_kvp_obj in inst_kvp_objs:
            key_obj = cls.get_rel_by_args('keys', _id=inst_kvp_obj['key'])[0]
            value_obj = cls.get_rel_by_args('values', _id=inst_kvp_obj['value'])[0]
            ret['key_objs'].append(KeysRender.render(key_obj))
            ret['value_objs'].append(ValuesRender.render(value_obj))
            ret['key_value_hash'][key_obj['key']] = value_obj['value']
        return ret

    # this doesn't get executed but is needed to be here to satisfy abstract method
    @classmethod
    def get_transactions(cls, **inst_obj):
        """Return the list of transaction ids for the user."""
        return [
            'transactions_{}'.format(transsip_obj['_id'])
            for transsip_obj in cls.get_rel_by_args('transsip', instrument=inst_obj['_id'])
        ]
