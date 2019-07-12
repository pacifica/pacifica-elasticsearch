#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Test the elasticsearch module."""
from unittest import TestCase
from time import sleep
import requests
from pacifica.elasticsearch.__main__ import main


class TestElasticsearch(TestCase):
    """Test the example class."""

    def test_main(self):
        """Test the add method in example class."""
        main('--objects-per-page', '4', '--threads', '1',
             '--exclude', 'keys.key=temp_f', '--time-ago', '3650 days after')
        sleep(3)
        resp = requests.post('http://localhost:9200/pacifica_search/_flush/synced')
        self.assertEqual(resp.status_code, 200)
        resp = requests.get('http://localhost:9200/pacifica_search/_stats')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()['indices']['pacifica_search']['primaries']['docs']['count'], 44)
        resp = requests.get('http://localhost:9200/pacifica_search/doc/transactions_67')
        self.assertEqual(resp.status_code, 200)
