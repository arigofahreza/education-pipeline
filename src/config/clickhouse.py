from starlette.config import Config


config = Config('.env')
CLICKHOUSE_HOST: str = config.get('CLICKHOUSE_HOST', cast=str)
CLICKHOUSE_PORT: int = config.get('CLICKHOUSE_PORT', cast=int)
CLICKHOUSE_USERNAME: str = config.get('CLICKHOUSE_USERNAME', cast=str)
CLICKHOUSE_PASSWORD: str = config.get('CLICKHOUSE_PASSWORD', cast=str)
CLICKHOUSE_DB: str = config.get('CLICKHOUSE_DB', cast=str)
