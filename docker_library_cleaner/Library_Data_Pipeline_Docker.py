"""
Library Data Quality Pipeline - Docker Version

Author: Geoff Daly (revised for Docker)
Date: 11/11/2025

Description:
    Automated data quality pipeline for library management system.
    Validates, cleans, transforms, and loads library checkout data.
    Docker-ready version that outputs to 'output' folder.

Usage:
    python library_data_pipeline_docker.py
    python library_data_pipeline_docker.py --books "data/03_Library_Systembook.csv" --customers "data/03_Library_SystemCustomers.csv"
    python library_data_pipeline_docker.py --output "cleaned_data"
    python library_data_pipeline_docker.py --help
"""

import pandas as pd
import numpy as np
from datetime import datetime
import argparse
import warnings
import os
warnings.filterwarnings('ignore')


# ============================================================================
# SECTION 1: FUNCTIONS
# ============================================================================

def fileLoader(books_path, customers_path):
    """
    Load CSV files for processing
    AM Task Requirement: fileLoader function

    Args:
        books_path: Path to books CSV file
        customers_path: Path to customers CSV file

    Returns:
        tuple: (books_df, customers_df)
    """
    print("\n📚 Loading data files...")
    print(f"  [debug] Current working directory: {os.getcwd()}")
    print(f"  [debug] Books path: {books_path}")
    print(f"  [debug] Customers path: {customers_path}")

    try:
        books_df = pd.read_csv(books_path)
        customers_df = pd.read_csv(customers_path)

        print(f"  ✓ Raw books rows loaded: {len(books_df)}")
        print(f"  ✓ Raw customers rows loaded: {len(customers_df)}")

        # Drop fully empty rows here so metrics start from "real" records
        books_empty = books_df.isna().all(axis=1).sum()
        customers_empty = customers_df.isna().all(axis=1).sum()

        if books_empty > 0:
            print(f"  • Dropping {books_empty} fully empty rows from books")
            books_df = books_df.dropna(how='all')

        if customers_empty > 0:
            print(f"  • Dropping {customers_empty} fully empty rows from customers")
            customers_df = customers_df.dropna(how='all')

        print(f"  ✓ Books data usable rows: {len(books_df)}")
        print(f"  ✓ Customers data usable rows: {len(customers_df)}")

        return books_df, customers_df

    except FileNotFoundError as e:
        print(f"  ❌ Error: File not found - {e}")
        raise
    except Exception as e:
        print(f"  ❌ Error loading files: {e}")
        raise


def duplicateCheck(df):
    """
    Check for and remove duplicate records.
    Keeps the first occurrence; removes exact duplicates on key fields.

    AM Task Requirement: duplicateCheck function

    Args:
        df: DataFrame to check for duplicates

    Returns:
        DataFrame: DataFrame with duplicates removed
    """
    print("\n🔍 Checking for duplicates...")

    # Work on a copy
    df = df.copy()

    initial_count = len(df)

    # Check for duplicates based on Books, Customer ID, and Checkout date
    duplicates = df.duplicated(subset=['Books', 'Customer ID', 'Book checkout'], keep='first')
    duplicate_count = int(duplicates.sum())

    # Remove duplicates
    df_clean = df.drop_duplicates(subset=['Books', 'Customer ID', 'Book checkout'], keep='first')

    print(f"  ✓ Duplicates found: {duplicate_count}")
    print(f"  ✓ Duplicates removed: {duplicate_count}")
    print(f"  ✓ Records remaining: {len(df_clean)}")

    return df_clean


def naCheck(df):
    """
    Check for and remove records with missing critical data
    AM Task Requirement: naCheck function

    Logic:
      - drops rows where BOTH Books and Customer ID are missing
      - keeps rows where only one is missing (but logs counts)

    Args:
        df: DataFrame to check for NaN values

    Returns:
        DataFrame: DataFrame with NaN records removed
    """
    print("\n🔍 Checking for missing values (NaN)...")

    df = df.copy()
    initial_count = len(df)

    # Normalise blanks -> NaN in key columns
    df['Books'] = df['Books'].astype(str).str.strip()
    df['Books'] = df['Books'].replace(['', 'nan', 'NaN'], np.nan)

    # Convert Customer ID to numeric if possible
    df['Customer ID'] = pd.to_numeric(df['Customer ID'], errors='coerce')

    na_books = df['Books'].isna().sum()
    na_customers = df['Customer ID'].isna().sum()

    print(f"  • Missing book titles: {na_books}")
    print(f"  • Missing customer IDs: {na_customers}")

    # Drop rows where BOTH key fields are missing
    both_missing = df['Books'].isna() & df['Customer ID'].isna()
    df_clean = df[~both_missing]

    removed_count = initial_count - len(df_clean)
    print(f"  ✓ Records with critical NaN removed (both missing): {removed_count}")
    print(f"  ✓ Records remaining: {len(df_clean)}")

    return df_clean


def dateCleaner(df):
    """
    Clean and format date fields, fix common date issues
    AM Task Requirement: dateCleaner function

    Changes:
      - strip quotes
      - correct obvious year typos (2063/2062 -> 2023)
      - correct impossible days (32/05, 31/02, 31/04) by adjusting to valid dates
      - convert to datetime, keeping invalids as NaT instead of dropping rows

    Args:
        df: DataFrame with date fields to clean

    Returns:
        DataFrame: DataFrame with cleaned dates
    """
    print("\n🧹 Cleaning date fields...")

    df = df.copy()

    # Standardise date text: strip quotes and whitespace, normalise empties
    for col in ['Book checkout', 'Book Returned']:
        df[col] = df[col].astype(str).str.replace('"', '', regex=False).str.strip()
        df[col] = df[col].replace(['', 'nan', 'NaN'], np.nan)

    print("  ✓ Removed extra quotes and normalised blanks in dates")

    # Fix obvious year typos (e.g. 10/04/2063 -> 10/04/2023)
    for col in ['Book checkout', 'Book Returned']:
        df[col] = df[col].str.replace('2063', '2023')
        df[col] = df[col].str.replace('2062', '2023')

    print("  ✓ Fixed future date typos (2063/2062 -> 2023)")

    # Fix impossible days by correction, not by dropping
    def fix_impossible(s):
        if not isinstance(s, str) or '/' not in s:
            return s
        try:
            d, m, y = s.split('/')
        except ValueError:
            return s

        if d == '32' and m == '05':  # 32/05 -> 31/05
            d = '31'
        if d == '31' and m == '02':  # 31/02 -> 28/02
            d = '28'
        if d == '31' and m == '04':  # 31/04 -> 30/04
            d = '30'
        return f"{d}/{m}/{y}"

    for col in ['Book checkout', 'Book Returned']:
        df[col] = df[col].apply(fix_impossible)

    print("  ✓ Corrected impossible day values (32/05, 31/02, 31/04)")

    # Strip whitespace from book titles
    df['Books'] = df['Books'].astype(str).str.strip()

    # Keep raw text copy for auditing
    for col in ['Book checkout', 'Book Returned']:
        df[col + '_raw'] = df[col]

    # Convert to datetime but DON'T drop invalids – leave as NaT
    for col in ['Book checkout', 'Book Returned']:
        df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce')

    invalid_checkout = df['Book checkout'].isna().sum()
    invalid_return = df['Book Returned'].isna().sum()
    print(f"  ✓ Dates converted to datetime format")
    print(f"    • Invalid checkout dates (NaT): {invalid_checkout}")
    print(f"    • Invalid return dates   (NaT): {invalid_return}")
    print(f"  ✓ Records remaining after date cleaning: {len(df)} (no rows dropped here)")

    return df


def dataEnrich(df):
    """
    Calculate days between checkout and return dates, add enrichment fields
    AM Task Requirement: dataEnrich() - Calculate days between dates

    Changes:
      - correct negative durations by swapping dates when both present
      - keep open loans (no Book Returned) instead of dropping
      - flag remaining negative durations instead of removing

    Args:
        df: DataFrame with date columns

    Returns:
        DataFrame: Enriched DataFrame with calculated fields
    """
    print("\n📊 Enriching data with calculated fields...")

    df = df.copy()

    # REQUIRED: Calculate loan duration (days between dates)
    df['loan_duration'] = (df['Book Returned'] - df['Book checkout']).dt.days

    # Detect negative durations (likely swapped dates)
    mask_negative = df['loan_duration'] < 0
    mask_both_dates = df['Book checkout'].notna() & df['Book Returned'].notna()
    swap_mask = mask_negative & mask_both_dates

    swapped_count = int(swap_mask.sum())
    if swapped_count > 0:
        # Swap checkout and return
        tmp_checkout = df.loc[swap_mask, 'Book checkout'].copy()
        df.loc[swap_mask, 'Book checkout'] = df.loc[swap_mask, 'Book Returned']
        df.loc[swap_mask, 'Book Returned'] = tmp_checkout

        # Recompute duration
        df['loan_duration'] = (df['Book Returned'] - df['Book checkout']).dt.days

        print(f"  ✓ Corrected {swapped_count} records with negative loan duration (swapped dates)")

    # Flag any remaining negatives (but keep them)
    df['negative_duration_flag'] = df['loan_duration'] < 0

    # Additional enrichments for better analysis
    df['checkout_date_iso'] = df['Book checkout'].dt.strftime('%Y-%m-%d')
    df['return_date_iso'] = df['Book Returned'].dt.strftime('%Y-%m-%d')

    # Calculate expected return date (14 days from checkout)
    df['expected_return_date'] = df['Book checkout'] + pd.Timedelta(days=14)

    # Calculate overdue days
    #  - if returned: overdue_days = returned - expected
    #  - if not returned and expected in past: overdue_days = today - expected
    today = pd.Timestamp('today').normalize()

    df['overdue_days'] = np.where(
        df['Book Returned'].notna() & df['expected_return_date'].notna(),
        (df['Book Returned'] - df['expected_return_date']).dt.days,
        np.where(
            df['Book Returned'].isna() & df['expected_return_date'].notna(),
            (today - df['expected_return_date']).dt.days,
            np.nan
        )
    )

    # Flag overdue books
    df['is_overdue'] = df['overdue_days'] > 0

    # Statistics
    avg_duration = df['loan_duration'].mean()
    overdue_count = int(df['is_overdue'].sum())
    overdue_pct = (overdue_count / len(df) * 100) if len(df) > 0 else 0

    print(f"  ✓ Added 7 calculated/flag fields:")
    print(f"    • loan_duration (required)")
    print(f"    • negative_duration_flag")
    print(f"    • checkout_date_iso (SQL compatible)")
    print(f"    • return_date_iso (SQL compatible)")
    print(f"    • expected_return_date")
    print(f"    • overdue_days")
    print(f"    • is_overdue")

    print(f"\n  📈 Statistics:")
    print(f"    • Average loan duration: {avg_duration:.1f} days")
    print(f"    • Overdue books: {overdue_count} ({overdue_pct:.1f}%)")

    # Note: we no longer drop records in this step
    return df


# ============================================================================
# SECTION 2: DATA ENGINEERING METRICS TRACKING
# ============================================================================

class DEMetrics:
    """Track Data Engineering metrics throughout the pipeline for a single table"""

    def __init__(self, table_name):
        self.table_name = table_name
        self.initial_rows = 0
        self.rows_after_duplicates = 0
        self.rows_after_na = 0
        self.rows_after_cleaning = 0
        self.final_rows = 0
        self.duplicates_removed = 0
        self.na_removed = 0
        self.invalid_dates_removed = 0
        self.negative_duration_removed = 0
        self.timestamp = datetime.now()

    def calculate_totals(self):
        """Calculate total drops"""
        self.total_dropped = self.initial_rows - self.final_rows
        self.retention_rate = (self.final_rows / self.initial_rows * 100) if self.initial_rows > 0 else 0

    def print_summary(self):
        """Print DE metrics summary"""
        self.calculate_totals()

        print("\n" + "="*70)
        title = f"DATA ENGINEERING METRICS SUMMARY - {self.table_name.upper()}"
        print(title.center(70))
        print("="*70)
        print(f"\n📊 Processing Metrics:")
        print(f"  Initial records loaded:        {self.initial_rows}")
        print(f"  After duplicate removal:       {self.rows_after_duplicates} (-{self.duplicates_removed})")
        print(f"  After NaN removal:             {self.rows_after_na} (-{self.na_removed})")
        print(f"  After date cleaning:           {self.rows_after_cleaning} (-{self.invalid_dates_removed})")
        print(f"  After enrichment:              {self.final_rows} (-{self.negative_duration_removed})")
        print(f"\n📈 Overall Summary:")
        print(f"  Total records dropped:         {self.total_dropped}")
        print(f"  Final records:                 {self.final_rows}")
        print(f"  Retention rate:                {self.retention_rate:.1f}%")
        print("="*70)

    def to_dict(self):
        """Convert metrics to dictionary for CSV export"""
        self.calculate_totals()
        return {
            'timestamp': self.timestamp,
            'table_name': self.table_name,
            'initial_records': self.initial_rows,
            'duplicates_removed': self.duplicates_removed,
            'na_removed': self.na_removed,
            'invalid_dates_removed': self.invalid_dates_removed,
            'negative_duration_removed': self.negative_duration_removed,
            'total_dropped': self.total_dropped,
            'final_records': self.final_rows,
            'retention_rate': f"{self.retention_rate:.1f}%"
        }


# ============================================================================
# SECTION 3: MAIN PIPELINE
# ============================================================================

def run_pipeline(books_path, customers_path, output_dir='output'):
    """
    Run the complete data quality pipeline

    Args:
        books_path: Path to books CSV file
        customers_path: Path to customers CSV file
        output_dir: Directory to save output files (default: 'output')

    Returns:
        tuple: (books_df, customers_df, (metrics_books, metrics_customers))
    """
    print("\n" + "="*70)
    print("LIBRARY DATA QUALITY PIPELINE - STARTING".center(70))
    print("="*70)

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    print(f"\n📁 Output directory: {os.path.abspath(output_dir)}")

    # Initialize metrics trackers
    metrics_books = DEMetrics(table_name='books')
    metrics_customers = DEMetrics(table_name='customers')

    # Step 1: Load data (fileLoader)
    books_df, customers_df = fileLoader(books_path, customers_path)

    # === BOOKS METRICS ===
    metrics_books.initial_rows = len(books_df)

    # Step 2: Remove duplicates (duplicateCheck)
    books_df = duplicateCheck(books_df)
    metrics_books.rows_after_duplicates = len(books_df)
    metrics_books.duplicates_removed = metrics_books.initial_rows - metrics_books.rows_after_duplicates

    # Step 3: Remove NaN values (naCheck)
    books_df = naCheck(books_df)
    metrics_books.rows_after_na = len(books_df)
    metrics_books.na_removed = metrics_books.rows_after_duplicates - metrics_books.rows_after_na

    # Step 4: Clean dates (dateCleaner)
    books_df = dateCleaner(books_df)
    metrics_books.rows_after_cleaning = len(books_df)
    # With current logic, we don't drop rows here, so this will usually be 0
    metrics_books.invalid_dates_removed = metrics_books.rows_after_na - metrics_books.rows_after_cleaning

    # Step 5: Enrich data (dataEnrich)
    books_df = dataEnrich(books_df)
    metrics_books.final_rows = len(books_df)
    # We also don't drop rows here, so this will usually be 0
    metrics_books.negative_duration_removed = metrics_books.rows_after_cleaning - metrics_books.final_rows

    # === CUSTOMERS METRICS ===
    print("\n👥 Cleaning customers data...")
    metrics_customers.initial_rows = len(customers_df)

    # At the moment we only drop fully empty rows for customers (already done in fileLoader)
    customers_df = customers_df.dropna(how='all')
    metrics_customers.rows_after_duplicates = metrics_customers.initial_rows   # no dup step yet
    metrics_customers.rows_after_na = len(customers_df)
    metrics_customers.na_removed = metrics_customers.initial_rows - metrics_customers.rows_after_na
    metrics_customers.rows_after_cleaning = metrics_customers.rows_after_na   # no date cleaning
    metrics_customers.final_rows = metrics_customers.rows_after_cleaning

    print(f"  ✓ Valid customers: {len(customers_df)}")

    # Step 6: Print metrics summaries per table
    metrics_books.print_summary()
    metrics_customers.print_summary()

    # Step 7: Save to CSV in output folder
    print(f"\n💾 Saving cleaned data to '{output_dir}' folder...")
    books_output_path = os.path.join(output_dir, 'books_cleaned_final.csv')
    customers_output_path = os.path.join(output_dir, 'customers_cleaned_final.csv')
    metrics_output_path = os.path.join(output_dir, 'processing_metrics.csv')
    
    books_df.to_csv(books_output_path, index=False)
    customers_df.to_csv(customers_output_path, index=False)
    
    # Also save metrics to CSV
    metrics_df = pd.DataFrame([m.to_dict() for m in [metrics_books, metrics_customers]])
    metrics_df.to_csv(metrics_output_path, index=False)
    
    print(f"  ✓ Saved: {books_output_path}")
    print(f"  ✓ Saved: {customers_output_path}")
    print(f"  ✓ Saved: {metrics_output_path}")

    print("\n" + "="*70)
    print("✅ PIPELINE COMPLETE".center(70))
    print("="*70)
    print(f"\n📂 Output files saved to: {os.path.abspath(output_dir)}/")
    print(f"   • books_cleaned_final.csv ({len(books_df)} records)")
    print(f"   • customers_cleaned_final.csv ({len(customers_df)} records)")
    print(f"   • processing_metrics.csv")

    return books_df, customers_df, (metrics_books, metrics_customers)


# ============================================================================
# SECTION 4: COMMAND LINE INTERFACE
# ============================================================================

def main():
    """
    Main function with argparse for command-line execution
    STRETCH GOAL: argparse library for CLI with file path specification
    """
    parser = argparse.ArgumentParser(
        description='Library Data Quality Pipeline - Process and clean library data (Docker Version)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with default paths:
  python %(prog)s

  # Specify custom file paths:
  python %(prog)s --books "data/03_Library_Systembook.csv" --customers "data/03_Library_SystemCustomers.csv"

  # Specify custom output directory:
  python %(prog)s --output "cleaned_data"
        """
    )

    # File path arguments
    parser.add_argument(
        '--books',
        default='data/03_Library_Systembook.csv',
        help='Path to books CSV file (default: data/03_Library_Systembook.csv)'
    )

    parser.add_argument(
        '--customers',
        default='data/03_Library_SystemCustomers.csv',
        help='Path to customers CSV file (default: data/03_Library_SystemCustomers.csv)'
    )

    # Output directory argument
    parser.add_argument(
        '--output',
        default='output',
        help='Output directory for cleaned CSV files (default: output)'
    )

    # Parse arguments
    args = parser.parse_args()

    # Run pipeline with provided arguments
    books_df, customers_df, (metrics_books, metrics_customers) = run_pipeline(
        books_path=args.books,
        customers_path=args.customers,
        output_dir=args.output
    )

    return books_df, customers_df, (metrics_books, metrics_customers)


if __name__ == "__main__":
    # Run with command-line arguments
    books_df, customers_df, metrics = main()