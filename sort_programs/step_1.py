import time
from concurrent.futures import ProcessPoolExecutor

import pandas as pd

from utils import SqlLiteConnection
from config import *


class Step1:
    def __init__(self):
        self.sql_con = SqlLiteConnection(f"../{DB_FILE}")

    def run(self):
        start_time = time.perf_counter()
        df: pd.DataFrame = self.get_ads()
        names_list = df[AdsTableCols.NAME.value].to_list()
        names_list = self.sorting_names(names_list)
        end_time = time.perf_counter()

        process_time = end_time - start_time

        self.store_in_db(names_list, process_time)
        self.sql_con.close_connection()

        print("finished")

    @staticmethod
    def sorting_names(names: list) -> list:
        names.sort()
        return names

    def store_in_db(self, names_list, process_time):
        df = pd.DataFrame({"sorting_step1": names_list})
        df["Sorting_step1_Process_time"] = process_time
        self.sql_con.dump_to_db(df, table_name=OUTPUT_TABLE_NAME, if_exists="replace")

    def get_ads(self):
        df = self.sql_con.query_db(QUERY_ADS)
        return df


if __name__ == "__main__":
    Step1().run()
