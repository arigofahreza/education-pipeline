from starlette.config import Config

config = Config('.env')

POSTGRE_HOST: str = config.get('POSTGRE_HOST', cast=str)
POSTGRE_PORT: int = config.get('POSTGRE_PORT', cast=int)
POSTGRE_USERNAME: str = config.get('POSTGRE_USERNAME', cast=str)
POSTGRE_PASSWORD: str = config.get('POSTGRE_PASSWORD', cast=str)
POSTGRE_DB: str = config.get('POSTGRE_DB', cast=str)
