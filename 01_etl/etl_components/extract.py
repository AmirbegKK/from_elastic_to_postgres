import datetime
from abc import abstractmethod
from dataclasses import dataclass
from operator import mod
from typing import Any

from redis import Redis
from psycopg2.extensions import connection as _connection

from etl_components import REDIS_HOST, REDIS_PORT


class RedisStorage:
    def __init__(self):
        self.redis_adapter = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    def save_state(self, key: str, value: Any) -> None:
        self.redis_adapter.set(key, value)
    
    def retrieve_state(self) -> dict:
        return self.redis_adapter

class State:

    def __init__(self, storage: RedisStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.storage.save_state(key, value)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        state = self.storage.retrieve_state()
        return state.get(key)


@dataclass
class Extractor:
    connection: _connection
    table: str

    def __post_init__(self):
        self.required_ids = []
        self.query = ""
        self.cur = self.connection.cursor()
        storage  = RedisStorage()
        self.state = State(storage)
        self.batch_size = 100

    @abstractmethod
    def extract(self) -> list:
        producer = PostgresProducer(self.connection, self.table)
        ids = producer.extract()

        if self.table != 'film_work':
            enricher = PostgresEnricher(self.connection, self.table)
            ids = enricher.extract(ids)
        
        merger = PostgresMerger(self.connection, self.table)
        return merger.extract(ids)


@dataclass
class PostgresProducer(Extractor):
    def extract(self) -> list:

        if (modified:= self.state.get_state(f'{self.table}_modified')) is None:
            modified = datetime.datetime(1970, 1, 1)
        else:
            modified = datetime.datetime.fromisoformat(modified)
        self.query = f"""
        SELECT id, modified
        FROM content.{self.table}
        WHERE modified > (%s)
        ORDER BY modified
        LIMIT {self.batch_size}; 
        """

        self.cur.execute(self.query, (modified,))
        data = self.cur.fetchall()
        self.state.set_state(f'{self.table}_modified', str(data[-1][-1]))
        self.required_ids.extend([row[0] for row in data])
        return self.required_ids

@dataclass
class PostgresEnricher(Extractor):
    
    def extract(self, ids: list) -> list:
        self.query = f"""
        SELECT fw.id, fw.modified
        FROM content.film_work fw
        LEFT JOIN content.{self.table}_film_work pgfw ON pgfw.film_work_id = fw.id
        WHERE pgfw.{self.table}_id::text = ANY(%s)
        ORDER BY fw.modified; 
        """
        self.cur.execute(self.query, (ids,))
        while data := self.cur.fetchmany(self.batch_size):
            self.required_ids.extend([row[0] for row in data])
        return self.required_ids


@dataclass
class PostgresMerger(Extractor):
    def extract(self, ids: list) -> list:

        self.query = f"""
        SELECT
            fw.id as fw_id, 
            fw.title, 
            fw.description, 
            fw.rating, 
            fw.type, 
            fw.created, 
            fw.modified, 
            pfw.role, 
            p.id, 
            p.full_name,
            g.name
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.id::text = ANY(%s);
        """

        self.cur.execute(self.query, (ids,))
        return self.cur.fetchall()