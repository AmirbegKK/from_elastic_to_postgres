import datetime
from abc import abstractmethod
from dataclasses import dataclass

from psycopg2.extensions import connection as _connection

from .state_config import RedisStorage, State

TABLES = ('genre', 'person', 'film_work')


@dataclass
class Extractor:
    connection: _connection

    def __post_init__(self):
        self.required_ids = []
        self.query = ''
        self.cur = self.connection.cursor()
        storage = RedisStorage()
        self.state = State(storage)
        self.batch_size = 5

    @abstractmethod
    def extract(self) -> list:
        producer = PostgresProducer(self.connection)
        films = producer.extract('film_work')
        genres = producer.extract('genre')
        persons = producer.extract('person')

        enricher = PostgresEnricher(self.connection)
        films = enricher.extract('genre', films, genres)
        films = enricher.extract('person', films, persons)
        self.required_ids = [_[1] for _ in films]

        merger = PostgresMerger(self.connection)
        return merger.extract(self.required_ids)


class PostgresProducer(Extractor):
    def extract(self, table) -> list:
        data = []
        if (modified := self.state.get_state(f'{table}_modified')) is None:
            modified = datetime.datetime(1970, 1, 1)
        else:
            modified = datetime.datetime.fromisoformat(modified)
        modified = datetime.datetime(1970, 1, 1)
        self.query = f"""
            SELECT id, modified
            FROM content.{table}
            WHERE modified > (%s)
            ORDER BY modified
            LIMIT {self.batch_size};
            """
        self.cur.execute(self.query, (modified,))
        if data := self.cur.fetchall():
            self.state.set_state(f'{table}_modified_temp', str(data[-1][-1]))
            return data


class PostgresEnricher(Extractor):

    def extract(self, table: str, templ_data: list, data: list) -> list:
        self.query = f"""
            SELECT fw.id, fw.modified
            FROM content.film_work fw
            LEFT JOIN content.{table}_film_work pgfw ON pgfw.film_work_id = fw.id
            WHERE pgfw.{table}_id::text = ANY(%s)
            ORDER BY fw.modified;
            """
        self.cur.execute(self.query, ([row[0] for row in data],))

        if data := self.cur.fetchall():
            for row in data:
                if row not in templ_data:
                    templ_data.append(row)
            #  Sort by moidifed
            templ_data.sort(key=lambda film_info: film_info[1])
        self.state.set_state(f'{table}_modified_temp', str(templ_data[-1][-1]))
        return templ_data


class PostgresMerger(Extractor):
    def extract(self, ids: list) -> list:

        self.query = """
        SELECT
            fw.id as fw_id,
            fw.title,
            fw.description,
            fw.rating,
            fw.type,
            CASE
                WHEN pfw.role = 'actor'
                THEN ARRAY_AGG(distinct p.full_name)
            END AS actors_names,
            CASE
                WHEN pfw.role = 'writer'
                THEN ARRAY_AGG(distinct p.full_name)
            END AS writers_names,
            CASE
                WHEN pfw.role = 'director'
                THEN ARRAY_AGG(distinct p.full_name)
            END AS director,
            CASE
                WHEN pfw.role = 'writer'
                THEN ARRAY_AGG(distinct p.id || ', ' || p.full_name)
            END AS writers,
            CASE
                WHEN pfw.role = 'actor'
                THEN ARRAY_AGG(distinct p.id || ', ' || p.full_name)
            END AS actors,
            ARRAY_AGG(distinct g.name) genres
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.id::text = ANY(%s)
        GROUP BY
            fw.id,
            fw.title,
            pfw.role;
        """

        self.cur.execute(self.query, (ids,))
        return self.cur.fetchall()
