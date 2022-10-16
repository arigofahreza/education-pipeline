import math
from hashlib import sha1
from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config.postgre import POSTGRE_USERNAME, POSTGRE_HOST, POSTGRE_PORT, POSTGRE_PASSWORD, POSTGRE_DB


def make_engine():
    url = f'postgresql+psycopg2://{POSTGRE_USERNAME}:{POSTGRE_PASSWORD}@{POSTGRE_HOST}:{POSTGRE_PORT}/{POSTGRE_DB}'
    engine = create_engine(url)

    Session = sessionmaker(bind=engine)
    session = Session()
    return session, engine


def generate_administrasi(adm: str) -> Optional[str]:
    adm_level = {
        'Provinsi': '1',
        'Kabupaten': '2',
        'Kota': '2',
        'Kecamatan': '3',
        'Desa': '4'
    }
    return adm_level.get(adm)


def generate_sector(sector_name: str) -> Optional[str]:
    sector = {
        'Agama': '1',
        'Infrastruktur': '2',
        'Kesejahteraan': '3',
        'Ekonomi': '4',
        'Ketenagakerjaan': '5',
        'Pendidikan': '6',
        'Politik': '7',
        'Kesehatan': '8',
        'Keamanan': '9'
    }

    return sector.get(sector_name)


def generate_partition_key(field: str) -> int:
    hm = sha1()
    hm.update(field.encode())
    res = int(hm.hexdigest(), 16)
    return int(res % 50)


def generate_floating(field: float, row, field_name):
    if isinstance(row[field_name], float) or isinstance(row[field_name], int):
        if not math.isnan(row[field_name]):
            field = row[field_name]
        else:
            field = None
    return field


def generate_integer(row, field_name) -> int:
    field = 0
    if not math.isnan(row[field_name]):
        field = row[field_name]
    return int(field)
