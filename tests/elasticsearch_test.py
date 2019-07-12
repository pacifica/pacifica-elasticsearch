#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Test the elasticsearch module."""
from unittest import TestCase
from pacifica.elasticsearch.__main__ import main


class TestElasticsearch(TestCase):
    """Test the example class."""

    def test_main(self):
        """Test the add method in example class."""
        hit_exception = False
        try:
            main('--objects-per-page=2', '--threads=1')
        # pylint: disable=broad-except
        except Exception:
            hit_exception = True
        # pylint: enable=broad-except
        self.assertFalse(hit_exception, 'even trying to run main help does not work')
