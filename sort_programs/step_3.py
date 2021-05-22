import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import pandas as pd

from utils import Step0, k_way_merge
from config import *


class Step3(Step0):
    def __init__(self):
        super().__init__(f"../{DB_FILE}")
        # super().__init__()

    def run(self):
        start_time = time.perf_counter()

        chunks_amount = int(RECORDS_NUM / CHUNK_SIZE)
        chunks_num = [i for i in range(chunks_amount)]
        with ThreadPoolExecutor() as executer:
            chunks = executer.map(self.chunk_handler, chunks_num)

        chunk_names = [chunk for chunk in chunks]
        merged_names = k_way_merge(*chunk_names)

        end_time = time.perf_counter()
        process_time = end_time - start_time
        self.store_in_db(merged_names, process_time, step_num=3)


if __name__ == "__main__":
    Step3().run()
