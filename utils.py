import abc
import pandas as pd
import sqlite3
import sys

sys.path.append("./")
from rafael.config import *


class SqlLiteConnection:
    def __init__(self, db_file=DB_FILE):
        self.conn = self._connect_to_db(db_file)

    @staticmethod
    def _connect_to_db(db_file: str):
        """ create a database connection to a SQLite database """
        conn = sqlite3.connect(db_file, check_same_thread=False)
        return conn

    def dump_to_db(self, df: pd.DataFrame, table_name: str, if_exists="append", index=False):
        """
        insert a Dataframe to db
        :param df: Dataframe to insert
        :param table_name: witch table to insert to
        :param if_exists: in case the table already exists how to insert the Dataframe
        :param index: to include index as column in table
        """
        df.to_sql(table_name, self.conn, index=index, if_exists=if_exists)

    def query_db(self, query: str) -> pd.DataFrame:
        """
        :param query:
        :return: Dataframe with query results
        """
        df = pd.read_sql(query, self.conn)
        return df

    def close_connection(self):
        """ close connection with db """
        self.conn.close()

    def get_connection(self):
        """ get the connection object """
        return self.conn


def k_way_merge(*args) -> list:
    merged_list = []
    list_iterators = [iter(li) for li in args]
    iterators_dict = {itr: next(itr) for itr in list_iterators}

    while True:
        if None not in iterators_dict.values():
            min_val = min(iterators_dict.values())
            for val in iterators_dict.values():
                if val == min_val:
                    merged_list.append(val)
        else:
            min_val = None

        iterators_dict = {itr: next(itr, None) if val == min_val else val for itr, val in iterators_dict.items()}
        iterators_dict = {itr: val for itr, val in iterators_dict.items() if val is not None}

        if len(iterators_dict) == 0:
            break

    return merged_list


class Step0:
    def __init__(self, db_file=DB_FILE):
        self.sql_con = SqlLiteConnection(db_file)
        self.db_file = db_file

    @abc.abstractmethod
    def run(self):
        pass

    @staticmethod
    def sort_names(df: pd.DataFrame, col_name=AdsTableCols.NAME.value) -> list:
        names_list = df[col_name].to_list()
        names_list.sort()
        return names_list

    def store_in_db(self, names_list, process_time, step_num, table_name=RESULTS_TABLE_NAME):
        try:
            df = self.sql_con.query_db(QUERY_RESULTS)
            if len(df) != len(names_list):
                raise Exception("Columns do not have the same length")
        except:
            df = pd.DataFrame()
        df[f"sorting_step{step_num}"] = names_list
        df[f"Sorting_step{step_num}_Process_time"] = process_time
        self.sql_con.dump_to_db(df, table_name=table_name, if_exists="replace")
        self.sql_con.close_connection()

    def get_ads(self, query=QUERY_ADS):
        df = self.sql_con.query_db(query)
        return df

    def chunk_handler(self, chunk_num=0, chunk_size=CHUNK_SIZE):
        query = f"{QUERY_ADS} as a where a.[index] between {chunk_num * chunk_size} and {(chunk_num + 1) * chunk_size - 1}"
        chunk_df = self.get_ads(query)
        names_list = self.sort_names(chunk_df)
        return names_list

    def other_chunk_handler(self, chunk_num=0, chunk_size=CHUNK_SIZE):
        query = f"{QUERY_ADS} as a where a.[index] between {chunk_num * chunk_size} and {(chunk_num + 1) * chunk_size - 1}"
        chunk_df = self.get_ads(query)
        return chunk_df

    @staticmethod
    def other_sort_names(df: pd.DataFrame, return_list=None, col_name=AdsTableCols.NAME.value):
        names_list = df[col_name].to_list()
        names_list.sort()
        return_list.append(names_list)
