import time
from concurrent.futures import ProcessPoolExecutor

import pandas as pd

from utils import SqlLiteConnection
from config import *


class Step1:
    def __init__(self):
        self.sql_con = SqlLiteConnection(f"../{DB_FILE}")

    def run(self):
        df: pd.DataFrame = self.get_ads()

        df = df.sort_values(by="name")
        self.sql_con.close_connection()

    def get_ads(self):
        df = self.sql_con.query_db(QUERY_ADS)
        return df


if __name__ == "__main__":
    Step1().run()
