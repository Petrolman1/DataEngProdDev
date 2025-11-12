"""
Pytest version of Calculator tests
Much simpler than unittest!
"""

from calculator_app import Calculator


def test_sum():
    """Test that addition works correctly"""
    calc = Calculator(10, 2)
    assert calc.get_sum() == 12, "the sum is wrong"


def test_difference():
    """Test that subtraction works correctly"""
    calc = Calculator(10, 2)
    assert calc.get_difference() == 8, "the difference is wrong"


def test_quotient():
    """Test that division works correctly"""
    calc = Calculator(10, 2)
    assert calc.get_quotient() == 5, "the quotient is wrong"


def test_product():
    """Test that multiplication works correctly"""
    calc = Calculator(10, 2)
    assert calc.get_product() == 20, "the product is wrong"


# Run with: python -m pytest test_demo/test_calculator_pytest.py -v