import abc
import pandas as pd
import sqlite3
import sys

try:
    sys.path.append("./")
    from rafael.config import *
except:
    from config import *


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


class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next


class MergeLists:
    def __init__(self, lists, convert_to_links=True):
        if convert_to_links:
            self.lists = [self.list_to_node(li) for li in lists]
        else:
            self.lists = lists

    def merge_k_lists(self) -> ListNode:
        lists = self.lists
        if not lists or len(lists) == 0:
            return None

        while len(lists) > 1:
            merged_lists = []

            for i in range(0, len(lists), 2):
                l1 = lists[i]
                l2 = lists[i + 1] if i + 1 < len(lists) else None
                merged_lists.append(self.merge_2_lists(l1, l2))
            lists = merged_lists
        merged_list = self.node_to_list(lists[0])
        return merged_list

    @staticmethod
    def merge_2_lists(l1: ListNode, l2: ListNode) -> ListNode:
        dummy = ListNode()
        tail = dummy

        while l1 and l2:
            if l1.val is None or l2.val is None:
                raise Exception("I do not allow null values in lists")
            if l1.val < l2.val:
                tail.next = l1
                l1 = l1.next
            else:
                tail.next = l2
                l2 = l2.next
            tail = tail.next
        if l1:
            tail.next = l1
        if l2:
            tail.next = l2
        return dummy.next

    @staticmethod
    def node_to_list(link: ListNode):
        """Takes a ListNode and converts to aa Python list"""
        my_list = []
        while link:
            my_list.append(link.val)
            link = link.next
        return my_list

    @staticmethod
    def list_to_node(lst):
        """Takes a Python list and returns a Link with the same elements."""
        prev = ListNode(val=lst[0])
        first = prev
        for i in range(1, len(lst)):
            current = ListNode(lst[i])
            prev.next = current
            prev = current
        return first


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
