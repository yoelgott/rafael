import sys
import pandas as pd
import datetime as dt
import random

sys.path.append("../")
from rafael.utils import SqlLiteConnection
from rafael.config import *


class CreateAdsTable:
    def __init__(self):
        self.df = pd.DataFrame()

    def run(self):
        self.generate_names()
        self.generate_ids()
        self.upload_to_db()

    def upload_to_db(self):
        conn = SqlLiteConnection(DB_FILE)
        conn.dump_to_db(self.df, ADS_TABLE_NAME, if_exists="replace", index=True)
        conn.close_connection()

    def generate_ids(self):
        def _gen_id(row):
            to_hash = f"{row.name}_{row['name']}"
            return hash(to_hash)

        col_name = AdsTableCols.ID.value
        self.df[col_name] = self.df.apply(_gen_id, axis=1)

    def generate_names(self):
        """ generating random strings """
        names_list = []
        for i in range(RECORDS_NUM):
            chars_amount = random.randint(MIN_CHARS_AMOUNT, MAX_CHARS_AMOUNT)
            name = "".join(random.choices(CHARS_POOL, k=chars_amount))
            names_list.append(name)

        col_name = AdsTableCols.NAME.value
        df = pd.DataFrame({col_name: names_list})
        self.df = df


if __name__ == "__main__":
    print(f"Started DB Setup at: {dt.datetime.now()}")
    CreateAdsTable().run()
    print(f"Finished DB Setup at: {dt.datetime.now()}")
