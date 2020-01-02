#!/usr/bin/python
# -*- coding: utf-8 -*-
"""This is the render object for the search interface."""
import importlib
from six import text_type
from .config import get_config

ELASTIC_INDEX = get_config().get('elasticsearch', 'index')


class SearchRender:
    """Search render class to contain methods."""

    @staticmethod
    def get_render_class(obj_cls):
        """Get the render class dynamically."""
        obj_mod = importlib.import_module('.render.{}'.format(obj_cls), 'pacifica.elasticsearch')
        parts = [obj_cls_part.capitalize() for obj_cls_part in obj_cls.split('_')]
        return getattr(obj_mod, '{}Render'.format(''.join(parts)))

    @classmethod
    def object_exclude(cls, obj_cls, obj, exclude):
        """Check if object is part of the exclude tuples."""
        for xobj_cls, xobj_attr, xvalue in exclude:
            try:
                xattr_type = type(obj[xobj_attr])
                typed_xvalue = xattr_type(xvalue)
            except (ValueError, KeyError):
                continue
            if xobj_cls == obj_cls and xobj_attr in obj and obj[xobj_attr] == typed_xvalue:
                return True
        return False

    @classmethod
    def generate(cls, obj_cls, objs, exclude):
        """generate the institution object."""
        render_cls = cls.get_render_class(obj_cls)
        for obj in objs:
            if cls.object_exclude(obj_cls, obj, exclude):
                continue
            yield {
                '_op_type': 'update',
                '_index': ELASTIC_INDEX,
                '_type': 'doc',
                '_id': render_cls.obj_id(**obj),
                'doc': render_cls.render(obj, True, obj_cls != 'transactions'),
                'doc_as_upsert': True
            }
            if obj_cls == 'projects':
                st_render_cls = cls.get_render_class('science_themes')
                yield {
                    '_op_type': 'update',
                    '_index': ELASTIC_INDEX,
                    '_type': 'doc',
                    '_id': text_type('science_themes_{}').format(obj['science_theme']),
                    'doc': st_render_cls.render(obj, True, True),
                    'doc_as_upsert': True
                }
