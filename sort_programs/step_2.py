import time
from concurrent.futures import ProcessPoolExecutor

import pandas as pd

from utils import Step0, k_way_merge
from config import *


class Step2(Step0):
    def __init__(self):
        super().__init__(f"../{DB_FILE}")

    def run(self):
        start_time = time.perf_counter()
        df: pd.DataFrame = self.get_ads()

        for i in range(10):
            self.chunk_handler(i)

        chunk_names = self.split_to_chunks(df[AdsTableCols.NAME.value])
        chunk_names = [self.sort_names(chunk) for chunk in chunk_names]
        merged_names = k_way_merge(*chunk_names)

        end_time = time.perf_counter()
        process_time = end_time - start_time

        self.store_in_db(merged_names, process_time, step_num=2)


if __name__ == "__main__":
    Step2().run()
