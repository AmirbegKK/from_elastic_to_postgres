class Transformer:
    def transform(self, data: list):
        transformed = []
        for row in data:
            doc = {
                'id': row['fw_id'],
                'imdb_rating': row['rating'],
                'genre': row['genres'],
                'title': row['title'],
                'description': row['description'],
                'director': row['director'][0] if row['director'] is not None else [],
                'actors_names': row['actors_names'][0] if row['actors_names'] is not None else [],
                'writers_names': row['writers_names'][0] if row['writers_names'] is not None else [],
                'actors': [{ 'id': actor.split(',')[0], 'name': actor.split(',')[1]} for actor in row['actors'][0]] if row['actors'] is not None else [],
                'writers': [{ 'id': writer.split(',')[0], 'name': writer.split(',')[1]} for writer in row['writers'][0]] if row['writers'] is not None else [],
            }
            transformed.append(doc)
        return transformed