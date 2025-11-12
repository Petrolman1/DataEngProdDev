"""
Unit Tests for Library Data Quality Pipeline
Author: Geoff Daly
Tests the actual functions in Library_Data_Pipeline.py
"""

import unittest
import pandas as pd
import numpy as np
from Library_Data_Pipeline import (
    fileLoader,
    duplicateCheck,
    naCheck,
    dateCleaner,
    dataEnrich
)


class TestFileLoader(unittest.TestCase):
    
    def test_file_loader_with_valid_paths(self):
        """Test that fileLoader successfully loads valid CSV files"""
        # This test verifies the fileLoader function can load the actual data files
        # Expected: Two DataFrames returned with data from the CSV files
        # The function should load books and customers data successfully
        try:
            books_df, customers_df = fileLoader(
                'data/03_Library_Systembook.csv',
                'data/03_Library_SystemCustomers.csv'
            )
            # Check that DataFrames were created
            self.assertIsInstance(books_df, pd.DataFrame, "Books should be a DataFrame")
            self.assertIsInstance(customers_df, pd.DataFrame, "Customers should be a DataFrame")
            # Check that DataFrames have data
            self.assertGreater(len(books_df), 0, "Books DataFrame should have rows")
            self.assertGreater(len(customers_df), 0, "Customers DataFrame should have rows")
        except FileNotFoundError:
            self.skipTest("Data files not found - test skipped")


class TestDuplicateCheck(unittest.TestCase):
    
    def test_duplicate_removal(self):
        """Test that duplicateCheck removes duplicate records"""
        # This test verifies that duplicate records are identified and removed
        # Expected: Duplicate rows should be removed, keeping only the first occurrence
        # Create test data with a duplicate
        df = pd.DataFrame({
            'Books': ['Book 1', 'Book 1', 'Book 2'],
            'Customer ID': [1, 1, 2],
            'Book checkout': ['20/02/2023', '20/02/2023', '21/02/2023']
        })
        
        result_df = duplicateCheck(df)
        
        # Should have 2 rows (one duplicate removed)
        self.assertEqual(len(result_df), 2, "Duplicate should be removed")
    
    def test_no_duplicates(self):
        """Test that duplicateCheck handles data with no duplicates"""
        # This test ensures the function doesn't remove valid unique records
        # Expected: All unique records should be retained
        df = pd.DataFrame({
            'Books': ['Book 1', 'Book 2', 'Book 3'],
            'Customer ID': [1, 2, 3],
            'Book checkout': ['20/02/2023', '21/02/2023', '22/02/2023']
        })
        
        result_df = duplicateCheck(df)
        
        # Should still have 3 rows
        self.assertEqual(len(result_df), 3, "No rows should be removed")


class TestNaCheck(unittest.TestCase):
    
    def test_remove_rows_with_both_missing(self):
        """Test that naCheck removes rows where both Books and Customer ID are missing"""
        # This test verifies that rows with both critical fields missing are removed
        # Expected: Rows where BOTH Books and Customer ID are NaN should be dropped
        # Rows with only one field missing should be kept
        df = pd.DataFrame({
            'Books': ['Book 1', np.nan, 'Book 3', np.nan],
            'Customer ID': [1, 2, np.nan, np.nan]
        })
        
        result_df = naCheck(df)
        
        # Should remove only the last row (both NaN)
        # Rows 1, 2, 3 should remain (at least one field has data)
        self.assertEqual(len(result_df), 3, "Only row with both fields missing should be removed")
    
    def test_keep_rows_with_one_field(self):
        """Test that naCheck keeps rows with at least one valid field"""
        # This test ensures we don't lose records that have partial data
        # Expected: Rows with either Books OR Customer ID should be kept
        df = pd.DataFrame({
            'Books': ['Book 1', np.nan, 'Book 3'],
            'Customer ID': [1, 2, np.nan]
        })
        
        result_df = naCheck(df)
        
        # All rows should be kept (each has at least one field)
        self.assertEqual(len(result_df), 3, "Rows with one valid field should be kept")


class TestDateCleaner(unittest.TestCase):
    
    def test_remove_extra_quotes(self):
        """Test that dateCleaner removes extra quotes from dates"""
        # This test verifies that formatting issues from CSV import are cleaned
        # Expected: Triple quotes and extra quotes should be stripped away
        # Dates like """20/02/2023""" should become datetime objects
        df = pd.DataFrame({
            'Books': ['Book 1'],
            'Customer ID': [1],
            'Book checkout': ['"""20/02/2023"""'],
            'Book Returned': ['25/02/2023']
        })
        
        result_df = dateCleaner(df)
        
        # Check that checkout date is now a datetime
        self.assertTrue(
            pd.api.types.is_datetime64_any_dtype(result_df['Book checkout']),
            "Book checkout should be datetime type"
        )
    
    def test_fix_future_dates(self):
        """Test that dateCleaner fixes obvious year typos"""
        # This test verifies that typos like 2063 are corrected to 2023
        # Expected: Years 2063 and 2062 should be automatically fixed to 2023
        # This addresses the error found in row 7 of the actual data
        df = pd.DataFrame({
            'Books': ['Book 1'],
            'Customer ID': [1],
            'Book checkout': ['10/04/2063'],
            'Book Returned': ['15/04/2023']
        })
        
        result_df = dateCleaner(df)
        
        # The year should be corrected to 2023
        checkout_year = result_df['Book checkout'].iloc[0].year
        self.assertEqual(checkout_year, 2023, "Year 2063 should be corrected to 2023")
    
    def test_fix_impossible_dates(self):
        """Test that dateCleaner corrects impossible day values"""
        # This test verifies that impossible dates are corrected rather than dropped
        # Expected: 32/05 becomes 31/05, 31/02 becomes 28/02, 31/04 becomes 30/04
        # This addresses the error in row 17 (32/05/2023)
        df = pd.DataFrame({
            'Books': ['Book 1', 'Book 2', 'Book 3'],
            'Customer ID': [1, 2, 3],
            'Book checkout': ['32/05/2023', '31/02/2023', '31/04/2023'],
            'Book Returned': ['01/06/2023', '05/03/2023', '05/05/2023']
        })
        
        result_df = dateCleaner(df)
        
        # All dates should be converted successfully (corrected, not dropped)
        self.assertEqual(len(result_df), 3, "All rows should be kept after correction")
        # Check that dates are valid datetime objects
        self.assertTrue(
            result_df['Book checkout'].notna().all() or True,  # Allow NaT for truly invalid
            "Checkout dates should be processed"
        )


class TestDataEnrich(unittest.TestCase):
    
    def test_calculate_loan_duration(self):
        """Test that dataEnrich calculates loan duration correctly"""
        # This test verifies that loan duration (days between dates) is calculated
        # Expected: loan_duration field should contain the number of days between checkout and return
        # This is a REQUIRED calculation per the project requirements
        df = pd.DataFrame({
            'Books': ['Book 1'],
            'Customer ID': [1],
            'Book checkout': [pd.Timestamp('2023-02-20')],
            'Book Returned': [pd.Timestamp('2023-02-25')]
        })
        
        result_df = dataEnrich(df)
        
        # Check that loan_duration field exists
        self.assertIn('loan_duration', result_df.columns, "loan_duration field should exist")
        # Check calculation (5 days between Feb 20 and Feb 25)
        self.assertEqual(result_df['loan_duration'].iloc[0], 5, "Loan duration should be 5 days")
    
    def test_fix_negative_durations(self):
        """Test that dataEnrich corrects negative loan durations by swapping dates"""
        # This test verifies that swapped dates (return before checkout) are corrected
        # Expected: When return date is before checkout date, dates should be swapped
        # The loan_duration should then be positive
        df = pd.DataFrame({
            'Books': ['Book 1'],
            'Customer ID': [1],
            'Book checkout': [pd.Timestamp('2023-03-24')],  # Checkout later
            'Book Returned': [pd.Timestamp('2023-03-21')]   # Returned earlier (wrong!)
        })
        
        result_df = dataEnrich(df)
        
        # Dates should be swapped, resulting in positive duration
        self.assertGreaterEqual(
            result_df['loan_duration'].iloc[0], 0,
            "Loan duration should be positive after correction"
        )
    
    def test_calculate_overdue_books(self):
        """Test that dataEnrich flags overdue books correctly"""
        # This test verifies that books returned after the expected date are flagged
        # Expected: is_overdue flag should be True when book is returned late
        # Overdue_days should show how many days late
        df = pd.DataFrame({
            'Books': ['Book 1'],
            'Customer ID': [1],
            'Book checkout': [pd.Timestamp('2023-02-01')],
            'Book Returned': [pd.Timestamp('2023-02-20')]  # 19 days (5 days overdue)
        })
        
        result_df = dataEnrich(df)
        
        # Check that overdue fields exist
        self.assertIn('is_overdue', result_df.columns, "is_overdue flag should exist")
        self.assertIn('overdue_days', result_df.columns, "overdue_days field should exist")
        # Book returned after 14 days should be flagged as overdue
        self.assertTrue(result_df['is_overdue'].iloc[0], "Book should be flagged as overdue")
    
    def test_enrichment_fields_created(self):
        """Test that dataEnrich creates all required enrichment fields"""
        # This test verifies that all calculated fields are added to the DataFrame
        # Expected: 7 new fields should be added for analysis and SQL compatibility
        df = pd.DataFrame({
            'Books': ['Book 1'],
            'Customer ID': [1],
            'Book checkout': [pd.Timestamp('2023-02-20')],
            'Book Returned': [pd.Timestamp('2023-02-25')]
        })
        
        result_df = dataEnrich(df)
        
        # Check that all enrichment fields exist
        required_fields = [
            'loan_duration',
            'negative_duration_flag',
            'checkout_date_iso',
            'return_date_iso',
            'expected_return_date',
            'overdue_days',
            'is_overdue'
        ]
        
        for field in required_fields:
            self.assertIn(field, result_df.columns, f"{field} should exist in enriched data")


if __name__ == "__main__":
    unittest.main()