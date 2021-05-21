from enum import Enum

DB_FILE = "db/rafael.db"

ADS_TABLE_NAME = "ads"


class AdsTableCols(Enum):
    INDEX = "index"
    ID = "id"
    NAME = "name"
