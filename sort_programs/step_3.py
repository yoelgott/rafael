import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import multiprocessing

import pandas as pd

from utils import Step0, k_way_merge
from config import *


class Step3(Step0):
    def __init__(self):
        super().__init__(f"../{DB_FILE}")
        self.step_num = 3

    def run(self):
        print(f"Started step number - {self.step_num}")
        start_time = time.perf_counter()

        chunks_amount = int(RECORDS_NUM / CHUNK_SIZE)
        with ThreadPoolExecutor() as executer:
            # chunk_names = list(executer.map(self.chunk_handler, range(chunks_amount)))

            processes = []
            manager = multiprocessing.Manager()
            return_list = manager.list()
            for chunk_df in executer.map(self.chunk_handler, range(chunks_amount)):
                p = multiprocessing.Process(target=self.sort_names, args=(chunk_df, return_list))
                p.start()
                processes.append(p)
            for process in processes:
                process.join()

        middle_time = time.perf_counter()
        mid_process_time = middle_time - start_time
        print("hello")
        merged_names = k_way_merge(*return_list)

        end_time = time.perf_counter()
        process_time = end_time - start_time
        self.store_in_db(merged_names, process_time, step_num=self.step_num)
        print(f"Started step number - {self.step_num} in {process_time} seconds")


if __name__ == "__main__":
    Step3().run()
