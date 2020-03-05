#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Test the elasticsearch module."""
import os
import sys
import subprocess
from unittest import TestCase
from time import sleep
import json
import jsonschema
import requests
from celery.bin.celery import main as celery_main


class TestElasticsearch(TestCase):
    """Test the example class."""

    env_hash = {
        'BROKER_URL': 'redis://127.0.0.1:6379/0',
        'BACKEND_URL': 'redis://127.0.0.1:6379/0',
        'NOTIFICATIONS_DISABLED': 'True',
        'ADMIN_USER_ID': '10',
        'CACHE_SIZE': '0'
    }

    @classmethod
    def setUpClass(cls):
        """Setup the celery worker process."""
        for key, value in cls.env_hash.items():
            os.environ[key] = value
        cls.celery_thread = subprocess.Popen([
            sys.executable, '-m',
            'celery', '-A', 'pacifica.elasticsearch.tasks', 'worker', '--pool', 'solo',
            '-l', 'info', '--quiet', '-b', 'redis://127.0.0.1:6379/0'
        ])
        sleep(3)

    @classmethod
    def tearDownClass(cls):
        """Teardown and remove all messages."""
        try:
            celery_main([
                'celery', '-A', 'pacifica.elasticsearch.tasks', 'control',
                '-b', 'redis://127.0.0.1:6379/0', 'shutdown'
            ])
        except SystemExit:
            pass
        cls.celery_thread.communicate()
        cls.celery_thread.wait()
        try:
            celery_main([
                'celery', '-A', 'pacifica.elasticsearch.tasks', '-b', 'redis://127.0.0.1:6379/0',
                '--force', 'purge'
            ])
        except SystemExit:
            pass
        for key, _value in cls.env_hash.items():
            del os.environ[key]

    def test_main_errors(self):
        """Test some of the command line failure conditions."""
        # The environment needs to be set before import
        # pylint: disable=import-outside-toplevel
        from pacifica.elasticsearch.__main__ import object_options, cmp_date_options
        with self.assertRaises(ValueError, msg='blarg is not a valid object to sync'):
            object_options('blarg')
        with self.assertRaises(ValueError, msg='blarg is not a valid date to compare objects to'):
            cmp_date_options('blarg')
        self.assertEqual(object_options('users'), 'users')
        self.assertEqual(cmp_date_options('created'), 'created')

    def test_main(self):
        """Test the add method in example class."""
        # The environment needs to be set before import
        # pylint: disable=import-outside-toplevel
        from pacifica.elasticsearch.__main__ import main
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
        # The environment needs to be set before import
        # pylint: disable=import-outside-toplevel
        from pacifica.elasticsearch.__main__ import main
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

    def test_main_celery_single_obj(self):
        """Test the add method in example class."""
        # The environment needs to be set before import
        # pylint: disable=import-outside-toplevel
        from pacifica.elasticsearch.__main__ import main
        main('--objects-per-page', '4', '--celery', '--object=projects',
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

    def test_keyword_query(self):
        """Test the keyword query for users."""
        self.test_main()
        for query_example in ['issue_9.json', 'issue_15_projects.json', 'issue_15_transactions.json', 'issue_23_projects.json']:
            post_data = json.loads(open(os.path.join(os.path.dirname(__file__), query_example)).read())
            resp = requests.post('http://localhost:9200/_search', json=post_data)
            bad_message = """
            Bad response code 200 != {}

            Error output from body:

            {}
            """
            self.assertEqual(resp.status_code, 200, bad_message.format(resp.status_code, resp.content))
