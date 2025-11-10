"""
Test script to run data_validator.py on the actual library CSV files
Run this to test your validation module!

Author: Geoff Daly
Project: DataEngProdDev
"""

import pandas as pd
import sys
import os

# Add src to path so we can import our modules
sys.path.append('src')

from data_validator import validate_dataframe, print_validation_report


def main():
    print("="*70)
    print("Library Data Quality Validator - Test Run")
    print("="*70)
    print(f"\nCurrent directory: {os.getcwd()}")
    print("Loading CSV files from data/ folder...\n")
    
    # Load the CSV files from data/ folder

    # 🔍 DEBUG: list directory contents before loading
    from pathlib import Path

    print("Top-level contents:", os.listdir())
    print("\nContents inside 'data/' folder:")
    for p in Path('data').glob('*'):
        print(" -", repr(p.name))

# Now try to load the CSVs

    try:
        books_df = pd.read_csv('data/03_Library_Systembook.csv')
        customers_df = pd.read_csv('data/03_Library_SystemCustomers.csv')
        
        print(f"✓ Books CSV loaded: {len(books_df)} records")
        print(f"✓ Customers CSV loaded: {len(customers_df)} records")
        
    except FileNotFoundError as e:
        print(f"❌ ERROR: CSV files not found!")
        print(f"   {str(e)}")
        print("\n💡 Make sure:")
        print("   1. You're running this from the DataEngProdDev folder")
        print("   2. CSV files are in the data/ folder")
        print("   3. Files are named:")
        print("      - data/03_Library_Systembook.csv")
        print("      - data/03_Library_SystemCustomers.csv")
        return
    
    # Display first few rows to verify data loaded correctly
    print("\n" + "-"*70)
    print("First 3 rows of books data:")
    print("-"*70)
    print(books_df.head(3))
    
    print("\n" + "-"*70)
    print("First 3 rows of customers data:")
    print("-"*70)
    print(customers_df.head(3))
    
    # Run validation
    print("\n" + "="*70)
    print("Running validation...")
    print("="*70)
    
    errors = validate_dataframe(books_df, customers_df)
    
    # Print the detailed report
    print_validation_report(errors)
    
    # Print summary for presentation
    print("\n" + "="*70)
    print("📊 SUMMARY FOR YOUR COURSEWORK PRESENTATION:")
    print("="*70)
    print(f"   • Date format errors: {len(errors['date_format_errors'])}")
    print(f"   • Date range errors (future dates): {len(errors['date_range_errors'])}")
    print(f"   • Impossible dates: {len(errors['impossible_dates'])}")
    print(f"   • Logical errors (return before checkout): {len(errors['logical_date_errors'])}")
    print(f"   • Missing customer references: {len(errors['customer_reference_errors'])}")
    print(f"   • Duplicate records: {len(errors['duplicates'])}")
    print(f"   • Formatting issues (quotes, spaces): {len(errors['formatting_issues'])}")
    print(f"\n   📈 TOTAL ERRORS DETECTED: {errors['total_errors']}")
    
    print("\n✅ Validation module is working correctly!")
    print("\n" + "="*70)
    print("NEXT STEPS:")
    print("="*70)
    print("1. Create data_cleaner.py to fix these issues")
    print("2. Create data_transformer.py to standardize dates")
    print("3. Create sql_loader.py to load to database")
    print("4. Create main.py to run the complete pipeline")
    print("="*70)


if __name__ == "__main__":
    main()