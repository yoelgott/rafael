import unittest
import numpy as np
import random
import string
import sys

sys.path.append("../")
from rafael.utils import MergeLists


class TestKMerge(unittest.TestCase):
    @staticmethod
    def assertListSorted(my_list: list):
        sorted_check = [my_list[i] <= my_list[i + 1] for i in range(len(my_list) - 1)]
        if not all(sorted_check):
            raise AssertionError("List is not sorted")

    @staticmethod
    def gen_string_list():
        chars_pool = string.digits + string.ascii_letters
        string_lists = []
        for _ in range(10):
            string_list = []
            for i in range(200):
                chars_amount = random.randint(5, 15)
                this_string = "".join(random.choices(chars_pool, k=chars_amount))
                string_list.append(this_string)
            string_list.sort()
            string_lists.append(string_list)
        return string_lists

    @staticmethod
    def gen_ints_lists():
        ints_lists = np.random.randint(-1000, 1000, size=[20, 80]).tolist()
        for li in ints_lists:
            li.sort()
        return ints_lists

    def test_k_merge(self):
        ints_lists = self.gen_ints_lists()
        string_lists = self.gen_string_list()

        self.assertListSorted(MergeLists(ints_lists).merge_k_lists())
        self.assertListSorted(MergeLists(string_lists).merge_k_lists())

    def test_2_merge(self):
        l1 = MergeLists.list_to_node([1, 5, 12, 78])
        l2 = MergeLists.list_to_node([9, 23, 50, 51])
        lst = MergeLists.node_to_list(MergeLists.merge_2_lists(l1, l2))
        self.assertListSorted(lst)

    def test_nones(self):
        nones_lists = [[1, 2, None, 78], [23, 25, 27, 98]]
        self.assertRaises(TypeError, MergeLists(nones_lists).merge_k_lists)


if __name__ == "__main__":
    unittest.main()
