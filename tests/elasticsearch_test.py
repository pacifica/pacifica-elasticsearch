#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Test the elasticsearch module."""
import os
from unittest import TestCase
from time import sleep
import json
import jsonschema
import requests
from pacifica.elasticsearch.__main__ import main, object_options, cmp_date_options


class TestElasticsearch(TestCase):
    """Test the example class."""

    def test_main_errors(self):
        """Test some of the command line failure conditions."""
        with self.assertRaises(ValueError, msg="blarg is not a valid object to sync"):
            object_options('blarg')
        with self.assertRaises(ValueError, msg='blarg is not a valid date to compare objects to'):
            cmp_date_options('blarg')
        self.assertEqual(object_options('users'), 'users')
        self.assertEqual(cmp_date_options('created'), 'created')

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

    def test_main_celery(self):
        """Test the add method in example class."""
        main('--objects-per-page', '4', '--celery',
             '--exclude', 'keys.key=temp_f', '--time-ago', '3650 days after')
        sleep(3)
        resp = requests.post('http://localhost:9200/pacifica_search/_flush/synced')
        self.assertEqual(resp.status_code, 200)
        resp = requests.get('http://localhost:9200/pacifica_search/_stats')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()['indices']['pacifica_search']['primaries']['docs']['count'], 44)
        resp = requests.get('http://localhost:9200/pacifica_search/doc/transactions_67')
        self.assertEqual(resp.status_code, 200)

    def test_document_formats(self):
        """Verify a project document is of a specific format."""
        self.test_main()
        patterns = {
            'transactions_67': 'transaction_jsonschema.json',
            'projects_1234a': 'project_jsonschema.json'
        }
        for obj_id, schema_file in patterns.items():
            json_schema = json.loads(open(os.path.join(os.path.dirname(__file__), schema_file)).read())
            resp = requests.get('http://localhost:9200/pacifica_search/doc/{}'.format(obj_id))
            self.assertEqual(
                resp.status_code, 200, '{} did not have a correct status code {}'.format(obj_id, resp.status_code)
            )
            json_data = resp.json()
            jsonschema.validate(json_data, json_schema)
