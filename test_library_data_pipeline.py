import os
import unittest
import tempfile
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

from Library_Data_Pipeline import (
    fileLoader,
    duplicateCheck,
    naCheck,
    dateCleaner,
    dataEnrich,
    DEMetrics,
)


class TestFileLoader(unittest.TestCase):
    def test_fileLoader_drops_fully_empty_rows(self):
        # Create temporary directory and CSV files
        with tempfile.TemporaryDirectory() as tmpdir:
            books_path = os.path.join(tmpdir, "books.csv")
            customers_path = os.path.join(tmpdir, "customers.csv")

            # Books: 3 rows, last is fully empty
            books_df = pd.DataFrame({
                "Books": ["Book A", "Book B", np.nan],
                "Customer ID": [1, 2, np.nan],
                "Book checkout": ["01/01/2023", "02/01/2023", np.nan],
                "Book Returned": ["05/01/2023", "06/01/2023", np.nan],
            })
            books_df.to_csv(books_path, index=False)

            # Customers: 2 rows, last fully empty
            customers_df = pd.DataFrame({
                "Customer ID": [1, np.nan],
                "Customer Name": ["Alice", np.nan],
            })
            customers_df.to_csv(customers_path, index=False)

            loaded_books, loaded_customers = fileLoader(books_path, customers_path)

            # Fully empty rows should be dropped
            self.assertEqual(len(loaded_books), 2)
            self.assertEqual(len(loaded_customers), 1)


class TestDuplicateCheck(unittest.TestCase):
    def test_duplicateCheck_removes_duplicates_on_key_columns(self):
        df = pd.DataFrame({
            "Books": ["Book A", "Book A", "Book B"],
            "Customer ID": [1, 1, 2],
            "Book checkout": ["01/01/2023", "01/01/2023", "02/01/2023"],
            "Book Returned": ["05/01/2023", "05/01/2023", "06/01/2023"],
        })

        cleaned = duplicateCheck(df)

        # Original: 3 rows, 1 exact duplicate on key subset
        self.assertEqual(len(df), 3)
        self.assertEqual(len(cleaned), 2)
        # Ensure the remaining rows are unique on key subset
        self.assertEqual(
            cleaned.duplicated(subset=["Books", "Customer ID", "Book checkout"]).sum(),
            0
        )


class TestNaCheck(unittest.TestCase):
    def test_naCheck_drops_rows_where_both_books_and_customer_missing(self):
        df = pd.DataFrame({
            "Books": ["Book A", np.nan, np.nan, "Book B"],
            "Customer ID": [1, np.nan, 3, np.nan],
            "Book checkout": ["01/01/2023"] * 4,
            "Book Returned": ["05/01/2023"] * 4,
        })

        cleaned = naCheck(df)

        # Row index 1 has BOTH Books and Customer ID missing -> should be dropped
        self.assertEqual(len(df), 4)
        self.assertEqual(len(cleaned), 3)
        # No remaining row should have both missing
        both_missing = cleaned["Books"].isna() & cleaned["Customer ID"].isna()
        self.assertFalse(both_missing.any())


class TestDateCleaner(unittest.TestCase):
    def test_dateCleaner_fixes_year_and_impossible_dates(self):
        df = pd.DataFrame({
            "Books": ["Book A", "Book B", "Book C"],
            "Customer ID": [1, 2, 3],
            "Book checkout": [
                '"10/04/2063"',   # bad year
                '32/05/2023',     # impossible day
                '31/02/2023',     # impossible day
            ],
            "Book Returned": [
                '"15/04/2063"',   # bad year
                '01/06/2023',
                '05/03/2023',
            ],
        })

        cleaned = dateCleaner(df)

        # All rows preserved
        self.assertEqual(len(cleaned), 3)

        # Years corrected from 2063 -> 2023
        self.assertEqual(cleaned.loc[0, "Book checkout"].year, 2023)
        self.assertEqual(cleaned.loc[0, "Book Returned"].year, 2023)

        # Days corrected (not necessarily verifying exact day, but ensure they're valid dates)
        self.assertIsInstance(cleaned.loc[1, "Book checkout"], pd.Timestamp)
        self.assertIsInstance(cleaned.loc[2, "Book checkout"], pd.Timestamp)

        # No rows dropped
        self.assertFalse(cleaned["Book checkout"].isna().all())


class TestDataEnrich(unittest.TestCase):
    def test_dataEnrich_swaps_negative_durations_and_keeps_open_loans(self):
        checkout = pd.to_datetime(["2023-01-10", "2023-01-05", "2023-01-01"])
        returned = pd.to_datetime(["2023-01-05", "2023-01-01", None])  # second is before checkout, third is open

        df = pd.DataFrame({
            "Books": ["Book A", "Book B", "Book C"],
            "Customer ID": [1, 2, 3],
            "Book checkout": [d.strftime("%d/%m/%Y") for d in checkout],
            "Book Returned": [
                d.strftime("%d/%m/%Y") if pd.notna(d) else np.nan
                for d in returned
            ],
        })

        # Run through dateCleaner first to get datetimes in correct columns
        df_clean = dateCleaner(df)
        enriched = dataEnrich(df_clean)

        # All rows preserved
        self.assertEqual(len(enriched), 3)

        # After enrichment, there should be no negative loan_duration for rows where both dates exist
        mask_both_dates = enriched["Book checkout"].notna() & enriched["Book Returned"].notna()
        self.assertTrue((enriched.loc[mask_both_dates, "loan_duration"] >= 0).all())

        # Open loan (last row) should have NaN loan_duration but still present
        self.assertTrue(pd.isna(enriched.loc[2, "loan_duration"]))
        self.assertEqual(enriched.loc[2, "Books"], "Book C")

        # Overdue flags exist
        self.assertIn("is_overdue", enriched.columns)
        self.assertIn("expected_return_date", enriched.columns)


class TestDEMetrics(unittest.TestCase):
    def test_DEMetrics_calculations(self):
        m = DEMetrics(table_name="books")

        # Simulate some counts
        m.initial_rows = 21
        m.rows_after_duplicates = 20
        m.duplicates_removed = 1
        m.rows_after_na = 19
        m.na_removed = 1
        m.rows_after_cleaning = 19
        m.invalid_dates_removed = 0
        m.final_rows = 19
        m.negative_duration_removed = 0

        m.calculate_totals()

        self.assertEqual(m.total_dropped, 2)      # 21 -> 19
        self.assertAlmostEqual(m.retention_rate, (19 / 21) * 100, places=5)

        d = m.to_dict()
        self.assertEqual(d["table_name"], "books")
        self.assertEqual(d["initial_records"], 21)
        self.assertEqual(d["final_records"], 19)
        self.assertEqual(d["duplicates_removed"], 1)
        self.assertEqual(d["na_removed"], 1)


if __name__ == "__main__":
    unittest.main()
