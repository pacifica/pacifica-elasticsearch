#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Search base class has some common data and logic."""
try:
    from functools import lru_cache
except ImportError:  # pragma: no cover only python 2
    from backports.functools_lru_cache import lru_cache
from functools import wraps
from pacifica.metadata.rest.objectinfo import ObjectInfoAPI
from pacifica.metadata.orm import Relationships
from ..config import get_config


_LRU_GLOBAL_ARGS = {
    'maxsize': get_config().getint('elasticsearch', 'cache_size'),
    'typed': False
}


def search_lru_cache(func):
    """Wrap a function with my lru cache args."""
    @lru_cache(**_LRU_GLOBAL_ARGS)
    @wraps(func)
    def wrapper(*args, **kwargs):
        """Calling inter function."""
        return func(*args, **kwargs)
    return wrapper


class SearchBase:
    """Search base class containing common data and logic."""

    fields = []
    rel_objs = []
    obj_type = 'unimplemented'
    releaser_uuid = str(Relationships.get(Relationships.name == 'authorized_releaser').uuid)
    search_required_uuid = str(Relationships.get(Relationships.name == 'search_required').uuid)

    @classmethod
    @search_lru_cache
    def get_rel_by_args(cls, mdobject, **kwargs):
        """Get the transaction user relationship."""
        obj_cls = ObjectInfoAPI.get_class_object_from_name(mdobject)
        return [obj.to_hash() for obj in obj_cls.select().where(obj_cls.where_clause(kwargs))]

    @classmethod
    def get_transactions(cls, **kwargs):  # pragma: no cover abstract method
        """Unimplemented in the base class."""
        raise NotImplementedError

    @classmethod
    def _transsip_transsap_merge(cls, trans_obj, key):
        ret = set()
        for trans_rel in ['transsip', 'transsap']:
            for rel_trans_obj in cls.get_rel_by_args(trans_rel, **trans_obj):
                ret.update([rel_trans_obj[key]])
        return ret

    @staticmethod
    def _cls_name_to_module(rel_cls):
        """Convert the class name to the object type and module to load."""
        return rel_cls.__module__.split('.')[-1]

    @classmethod
    def render(cls, obj, render_rel_objs=False, render_trans_ids=False):
        """Render the object and return it."""
        ret = {'type': cls._cls_name_to_module(cls)}
        for key in cls.fields:
            ret[key] = getattr(cls, key)(**obj)
        if render_rel_objs:
            for related_obj_name in cls.rel_objs:
                ret[related_obj_name] = getattr(cls, '{}_obj_lists'.format(related_obj_name))(**obj)
        if render_trans_ids:
            trans_id_list = cls.get_transactions(**obj)
            ret['transaction_ids'] = trans_id_list
            ret['transaction_count'] = len(trans_id_list)
        return ret
