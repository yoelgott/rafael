import time
from concurrent.futures import ThreadPoolExecutor
import multiprocessing

from utils import Step0, MergeLists
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
            chunk_names = list(executer.map(self.chunk_handler, range(chunks_amount)))
        merged_names = MergeLists(chunk_names).merge_k_lists()

        process_time = time.perf_counter() - start_time
        self.store_in_db(merged_names, process_time, step_num=self.step_num)
        print(f"Started step number - {self.step_num} in {process_time} seconds")

    def proposed_parallel_approach(self):
        """
        amlak - this function didn't speed up the process so I didn't implement it in my code

        I tried to use threading and multiprocessing together because there are 2 different kinds
         of functionalities here:
        1) access the db - witch usually should be implemented with threading
        2) sort the list - witch usually should be implemented with multiprocessing
        """
        chunks_amount = int(RECORDS_NUM / CHUNK_SIZE)
        with ThreadPoolExecutor() as executer:
            processes = []
            manager = multiprocessing.Manager()
            return_list = manager.list()
            for chunk_df in executer.map(self.other_chunk_handler, range(chunks_amount)):
                p = multiprocessing.Process(target=self.other_sort_names, args=(chunk_df, return_list))
                p.start()
                processes.append(p)
            for process in processes:
                process.join()
        return list(return_list)


if __name__ == "__main__":
    Step3().run()
