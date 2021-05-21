from enum import Enum

DB_FILE = "db/rafael.db"

ADS_TABLE_NAME = "ads"
OUTPUT_TABLE_NAME = "results"

CHUNK_SIZE = 2000

QUERY_ADS = """
    select *
    from ads
"""


class AdsTableCols(Enum):
    INDEX = "index"
    ID = "id"
    NAME = "name"
