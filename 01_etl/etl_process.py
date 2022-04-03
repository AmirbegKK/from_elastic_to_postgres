import logging
from contextlib import closing
from time import sleep

import backoff
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from etl_components import DSL
from etl_components.extract import Extractor
from etl_components.transform import Transformer
from etl_components.load_to_es import Loader

logger = logging.getLogger()

TABLES = ('genre', 'person', 'film_work')


@backoff.on_exception(backoff.expo, Exception)
def load():
    loader = Loader()


@backoff.on_exception(backoff.expo, Exception)
def transform(data):
    transformer = Transformer()
    return transformer.transform(data)


@backoff.on_exception(backoff.expo, AssertionError, max_tries=1)
def extract(connection: _connection, table: str):
    extractor = Extractor(connection, table)
    return extractor.extract()


@backoff.on_exception(backoff.expo, AssertionError)
def run_etl_process(table: str):
    with closing(psycopg2.connect(**DSL, cursor_factory=DictCursor)) as pg_conn:
        load(transform(extract(pg_conn, table)))


if __name__ == '__main__':
    while True:
        for table in TABLES:
            run_etl_process(table)
        sleep(0.5)
