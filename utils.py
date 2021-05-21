import abc
import time
import pandas as pd
import sqlite3

from config import *


class SqlLiteConnection:
    def __init__(self, db_file="db/rafael.db"):
        self.conn = self._connect_to_db(db_file)

    @staticmethod
    def _connect_to_db(db_file: str):
        """ create a database connection to a SQLite database """
        conn = sqlite3.connect(db_file)
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
    current_values = [next(itr) for itr in list_iterators]

    while True:
        min_val = min(current_values)
        for val in current_values:
            if val == min_val:
                merged_list.append(val)
        current_values = [next(itr, None) if current_values[i] == min_val else current_values[i] for i, itr in
                          enumerate(list_iterators)]
        if not any(current_values):
            break
    return merged_list


class Step0:
    def __init__(self, db_file=DB_FILE):
        self.sql_con = SqlLiteConnection(db_file)

    @abc.abstractmethod
    def run(self):
        pass

    @staticmethod
    def sorting_names(names: list) -> list:
        names.sort()
        return names

    def store_in_db(self, names_list, process_time, step_num):
        df = pd.DataFrame({f"sorting_step{step_num}": names_list})
        df[f"Sorting_step{step_num}_Process_time"] = process_time
        self.sql_con.dump_to_db(df, table_name=OUTPUT_TABLE_NAME, if_exists="replace")

    def get_ads(self):
        df = self.sql_con.query_db(QUERY_ADS)
        return df
