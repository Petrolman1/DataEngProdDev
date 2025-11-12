"""
Unit Tests for Library Data Quality Pipeline
Author: Geoff Daly
"""

import unittest
import pandas as pd
from library_data_pipeline import (
    validate_date_format,
    validate_date_range,
    validate_impossible_dates,
    validate_return_after_checkout,
    validate_customer_reference,
    clean_date_formatting,
    remove_empty_rows
)


class TestDateValidation(unittest.TestCase):
    
    def test_valid_date_format(self):
        """Test that valid date format passes validation"""
        is_valid, error = validate_date_format("20/02/2023")
        self.assertTrue(is_valid, "Valid date format should pass")
        self.assertIsNone(error, "Valid date should have no error message")
    
    def test_invalid_date_format(self):
        """Test that invalid date format fails validation"""
        is_valid, error = validate_date_format("2023-02-20")
        self.assertFalse(is_valid, "Invalid date format should fail")
        self.assertIsNotNone(error, "Invalid date should have error message")
    
    def test_impossible_date(self):
        """Test that impossible dates are detected"""
        is_valid, error = validate_impossible_dates("32/05/2023")
        self.assertFalse(is_valid, "Impossible date should fail validation")
    
    def test_future_date(self):
        """Test that future dates are detected"""
        is_valid, error = validate_date_range("10/04/2063")
        self.assertFalse(is_valid, "Future date should fail validation")


class TestLogicalValidation(unittest.TestCase):
    
    def test_return_after_checkout(self):
        """Test that return date after checkout date is valid"""
        is_valid, error = validate_return_after_checkout("20/02/2023", "25/02/2023")
        self.assertTrue(is_valid, "Return after checkout should be valid")
    
    def test_return_before_checkout(self):
        """Test that return date before checkout date is invalid"""
        is_valid, error = validate_return_after_checkout("24/03/2023", "21/03/2023")
        self.assertFalse(is_valid, "Return before checkout should be invalid")


class TestCustomerValidation(unittest.TestCase):
    
    def setUp(self):
        """Set up test customer data"""
        self.customers_df = pd.DataFrame({
            'Customer ID': [1, 2, 3],
            'Customer Name': ['Jane Doe', 'John Smith', 'Dan Reeves']
        })
    
    def test_valid_customer_id(self):
        """Test that valid customer ID passes validation"""
        is_valid, error = validate_customer_reference(1, self.customers_df)
        self.assertTrue(is_valid, "Valid customer ID should pass")
    
    def test_invalid_customer_id(self):
        """Test that invalid customer ID fails validation"""
        is_valid, error = validate_customer_reference(999, self.customers_df)
        self.assertFalse(is_valid, "Invalid customer ID should fail")


class TestDataCleaning(unittest.TestCase):
    
    def test_clean_date_formatting(self):
        """Test that extra quotes are removed from dates"""
        df = pd.DataFrame({
            'Book checkout': ['"""20/02/2023"""', '"24/03/2023"'],
            'Book Returned': ['25/02/2023', '21/03/2023']
        })
        cleaned_df = clean_date_formatting(df)
        self.assertEqual(cleaned_df['Book checkout'].iloc[0], '20/02/2023', 
                        "Extra quotes should be removed")
    
    def test_remove_empty_rows(self):
        """Test that empty rows are removed"""
        df = pd.DataFrame({
            'Books': ['Book 1', None, 'Book 2'],
            'Customer ID': [1, None, 2]
        })
        cleaned_df = remove_empty_rows(df)
        self.assertEqual(len(cleaned_df), 2, "Empty rows should be removed")


if __name__ == "__main__":
    unittest.main()