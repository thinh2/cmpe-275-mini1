from enum import Enum

class Filter:
    def __init__(self, col, op, val):
        self.cmp_val = val
        self.op = op
        self.col = col

    def execute(self, val): 
        match self.op:
            case FilterOp.EQ:
                return (val == self.cmp_val)
            case FilterOp.GT:
                return (val > self.cmp_val)
            case FilterOp.GTE:
                return (val >= self.cmp_val)
            case FilterOp.LT:
                return (val < self.cmp_val)
            case FilterOp.LTE:
                return (val <= self.cmp_val)

class FilterOp(Enum):
    EQ  = 1
    GT  = 2
    GTE = 3
    LT  = 4 
    LTE = 5