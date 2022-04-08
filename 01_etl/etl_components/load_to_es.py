from elasticsearch import Elasticsearch, helpers
from etl_components import ELASTIC_DSL

class Loader:

    def __init__(self):
        self.elastic = Elasticsearch(f"http://{ELASTIC_DSL['host']}:{ELASTIC_DSL['port']}")

    def load(self, data: list, index: str) -> None:

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
