import os

def env(key: str) -> str:
    return os.environ[key]

DSL = {
        'dbname': env('DB_NAME'),
        'user': env('DB_USER'),
        'password': env('DB_PASSWORD'),
        'host': env('DB_HOST'),
        'port': env('DB_PORT'),
        'options': '-c search_path=content',
    }
REDIS_HOST = env('REDIS_HOST')
REDIS_PORT = env('REDIS_PORT')