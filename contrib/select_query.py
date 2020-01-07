#!/usr/bin/python
from datetime import datetime, timedelta
from os.path import join, realpath
import json
import pacifica
pacifica.__path__.append(join(realpath('.'), 'pacifica'))

from pacifica.elasticsearch.search_sync import try_es_connect, es_client
from pacifica.elasticsearch.render.transactions import TransactionsRender
from pacifica.metadata.orm import Transactions
import logging

logging.basicConfig()
LOGGER = logging.getLogger('peewee')
LOGGER.setLevel('DEBUG')
try_es_connect()

ten_years_ago = datetime.now() - timedelta(days=3650)
one_day_ago = datetime.now() - timedelta(days=1)
one_day = datetime.now() + timedelta(days=1)
print(one_day.isoformat())
print(one_day_ago.isoformat())
print(json.dumps([ t.to_hash() for t in TransactionsRender.get_select_query(time_delta=ten_years_ago, obj_cls=Transactions, items_per_page=4, page=1)], indent=4))