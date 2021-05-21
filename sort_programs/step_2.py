import time
from concurrent.futures import ProcessPoolExecutor

import pandas as pd

from utils import Step0
from config import *


class Step2(Step0):
    def __init__(self):
        super().__init__(f"../{DB_FILE}")

    def run(self):
        start_time = time.perf_counter()
        df: pd.DataFrame = self.get_ads()

        chunk_names = self.split_to_chunks(df[AdsTableCols.NAME.value])

        end_time = time.perf_counter()

        process_time = end_time - start_time

        self.store_in_db(names_list, process_time, step_num=1)
        self.sql_con.close_connection()


if __name__ == "__main__":
    Step2().run()
