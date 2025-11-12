import unittest
from calculator_app import Calculator

class TestOperations(unittest.TestCase):
    
    def test_sum(self):
        calc = Calculator(10,2)
        self.assertEqual(calc.get_sum(), 12, "the sum is wrong")

    def test_difference(self):
        calc = Calculator(10,2)
        self.assertEqual(calc.get_difference(), 8, "the difference is wrong")

    def test_quotient(self):
        calc = Calculator(10,2)
        self.assertequal(calc.get_quotient(), 5, "the quotient is wrong")

    def test_sum(self):
        calc = Calculator(10,2)
        self.assertEqual(calc.get_product(), 20, "the product is wrong")    

if __name__ == "__main__":
    unittest.main()        