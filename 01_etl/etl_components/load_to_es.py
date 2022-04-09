from elasticsearch import Elasticsearch, helpers
from etl_components import ELASTIC_DSL
from state_config import RedisStorage, State


class Loader:

    def __init__(self):
        self.elastic = Elasticsearch(f"http://{ELASTIC_DSL['host']}:{ELASTIC_DSL['port']}")
        storage = RedisStorage()
        self.state = State(storage)

    def load(self, data: list, index: str, table: str) -> None:

        def gen_data():
            for doc in data:
                record = {
                    '_id': doc['id'],
                    '_op_type': 'index',
                    '_index': index,
                    **doc,
                }
                yield record

        helpers.bulk(client=self.elastic, actions=gen_data())
        state = self.state.get_state(f'{table}_modified_temp')
        self.state.set_state(f'{table}_modified', state)
