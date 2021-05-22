import time
import pandas as pd

from utils import Step0
from config import *


class Step1(Step0):
    def __init__(self):
        super().__init__(f"../{DB_FILE}")
        self.step_num = 1

    def run(self):
        print(f"Started step number - {self.step_num}")
        start_time = time.perf_counter()
        df: pd.DataFrame = self.get_ads()
        names_list = self.sort_names(df)
        end_time = time.perf_counter()
        process_time = end_time - start_time
        self.store_in_db(names_list, process_time, step_num=1)
        print(f"Started step number - {self.step_num} in {process_time} seconds")


if __name__ == "__main__":
    Step1().run()
