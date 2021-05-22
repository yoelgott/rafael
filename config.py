from enum import Enum
import string

RECORDS_NUM = 20000
MIN_CHARS_AMOUNT = 5
MAX_CHARS_AMOUNT = 15
CHARS_POOL = string.digits + string.ascii_letters

CHUNK_SIZE = 2000

DB_FILE = "db/rafael.db"
ADS_TABLE_NAME = "ads"
RESULTS_TABLE_NAME = "results"

QUERY_ADS = f"""select * from {ADS_TABLE_NAME}"""
QUERY_RESULTS = f"""select * from {RESULTS_TABLE_NAME}"""


class AdsTableCols(Enum):
    INDEX = "index"
    ID = "id"
    NAME = "name"
