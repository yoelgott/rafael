import unittest
import numpy as np
import random
import string
import sys

sys.path.append("../")
from rafael.utils import k_way_merge


class TestKMerge(unittest.TestCase):
    @staticmethod
    def assertListSorted(my_list: list):
        sorted_check = [my_list[i] <= my_list[i + 1] for i in range(len(my_list) - 1)]
        if not all(sorted_check):
            raise AssertionError("List is not sorted")

    def test_ints(self):
        ints_lists = np.random.randint(-1000, 1000, size=[20, 80]).tolist()
        for li in ints_lists:
            li.sort()
        self.assertListSorted(k_way_merge(*ints_lists))

    def test_strings(self):
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
        self.assertListSorted(k_way_merge(*string_lists))

    def test_nones(self):
        nones_lists = [[None] * 30] * 20
        self.assertListEqual(k_way_merge(*nones_lists), [])


if __name__ == "__main__":
    unittest.main()
