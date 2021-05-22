import unittest
import numpy as np
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


if __name__ == "__main__":
    unittest.main()
