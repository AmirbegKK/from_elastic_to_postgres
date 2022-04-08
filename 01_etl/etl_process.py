import logging
from contextlib import closing
from time import sleep

import backoff
import elasticsearch as es
import psycopg2
from etl_components import DSL
from etl_components.extract import Extractor
from etl_components.load_to_es import Loader
from etl_components.transform import Transformer
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

logger = logging.getLogger()

TABLES = ('genre', 'person', 'film_work')


@backoff.on_exception(backoff.expo, (es.exceptions.ConnectionTimeout, es.exceptions.ConnectionError), max_tries=3)
def load(data: dict):
    logger.info('Starting loader ...')
    loader = Loader()
    return loader.load(data, 'movies')


@backoff.on_exception(backoff.expo, (psycopg2.OperationalError, psycopg2.DatabaseError), max_tries=3)
def transform(data):
    logger.info('Starting transformer ...')
    transformer = Transformer()
    return transformer.transform(data)


@backoff.on_exception(backoff.expo, (psycopg2.OperationalError, psycopg2.DatabaseError), max_tries=3)
def extract(connection: _connection, table: str):
    logger.info('Starting extractor...')
    extractor = Extractor(connection, table)
    return extractor.extract()


@backoff.on_exception(backoff.expo, (psycopg2.OperationalError, psycopg2.DatabaseError), max_tries=3)
def run_etl_process(table: str):
    logger.info(f'Starting etl process for {table}')
    with closing(psycopg2.connect(**DSL, cursor_factory=DictCursor)) as pg_conn:
        load(transform(extract(pg_conn, table)))


if __name__ == '__main__':
    while True:
        for table in TABLES:
            run_etl_process(table)
            sleep(3)
        sleep(3)
