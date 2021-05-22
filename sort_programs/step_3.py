import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import pandas as pd

from utils import Step0, k_way_merge
from config import *


class Step3(Step0):
    def __init__(self):
        super().__init__(f"../{DB_FILE}")

    def run(self):
        start_time = time.perf_counter()

        chunks_amount = int(RECORDS_NUM / CHUNK_SIZE)
        with ThreadPoolExecutor() as executer:
            chunk_names = list(executer.map(self.chunk_handler, range(chunks_amount)))
        merged_names = k_way_merge(*chunk_names)

        end_time = time.perf_counter()
        process_time = end_time - start_time
        self.store_in_db(merged_names, process_time, step_num=3)


if __name__ == "__main__":
    Step3().run()
