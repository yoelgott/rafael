import time

from utils import Step0, MergeLists
from config import *


class Step2(Step0):
    def __init__(self):
        super().__init__(f"../{DB_FILE}")
        self.step_num = 2

    def run(self):
        print(f"Started step number - {self.step_num}")
        start_time = time.perf_counter()
        chunks_amount = int(RECORDS_NUM / CHUNK_SIZE)
        chunk_names = [self.chunk_handler(i) for i in range(chunks_amount)]
        merged_names = MergeLists(chunk_names).merge_k_lists()
        process_time = time.perf_counter() - start_time
        self.store_in_db(merged_names, process_time, step_num=self.step_num)
        print(f"Started step number - {self.step_num} in {process_time} seconds")


if __name__ == "__main__":
    Step2().run()
