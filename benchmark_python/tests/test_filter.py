import unittest
from src.core import Filter, FilterOp

class FilterTest(unittest.TestCase):
    def test_filter_greater(self):
        input = [3, 4, 5, 1]
        expected_result = [True, True, True, False]
        output = []
        val = 2
        filter = Filter("test", FilterOp.GT, val) 
        for val in input:
            output.append(filter.execute(val))
        
        self.assertEqual(output, expected_result)

    def test_filter_equal(self):
        input = [3, 4, 3, 1]
        expected_result = [True, False, True, False]
        output = []
        val = 3
        filter = Filter("test", FilterOp.EQ, val) 
        for val in input:
            output.append(filter.execute(val))
        
        self.assertEqual(output, expected_result)

