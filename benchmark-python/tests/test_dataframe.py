import os
import sys
from src.core import Data 
from src.core import Filter, FilterOp
import unittest

class TestDataFrame(unittest.TestCase):

    def setUp(self) -> None:
        self.test_data_path = f"{os.getcwd()}/tests/testdata"

    def test_non_null_csv(self):
        data = Data(f"{self.test_data_path}/header_no_null.csv")
        filter_query = Filter('age', FilterOp.GTE, 13)
        expected_output = 2
        self.assertEqual(data.filter(filter_query), expected_output)

        filter_eq = Filter('name', FilterOp.EQ, 'TT')
        self.assertEqual(data.filter(filter_eq), 1)

    def test_null_csv(self):
        data = Data(f"{self.test_data_path}/header_with_null_value.csv")
        filter_query = Filter('age', FilterOp.GTE, 13)
        expected_output = 4
        self.assertEqual(data.filter(filter_query), expected_output)

        filter_eq = Filter('name', FilterOp.EQ, 'TT')
        self.assertEqual(data.filter(filter_eq), 2)

    def test_null_csv_multithread(self):
        data = Data(f"{self.test_data_path}/header_with_null_value.csv")
        filter_query = Filter('age', FilterOp.GTE, 13)
        expected_output = 4
        self.assertEqual(data.filter_multithread(filter_query), expected_output)

        filter_eq = Filter('name', FilterOp.EQ, 'TT')
        self.assertEqual(data.filter_multithread(filter_eq), 2)
