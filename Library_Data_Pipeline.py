"""
Library Data Quality Pipeline - Final Version

Author: Geoff Daly (Enhanced)
Date: 11/11/2025

Description:
    Automated data quality pipeline for library management system.
    Validates, cleans, transforms, and loads library checkout data.

Usage:
    python library_pipeline_final.py
    python library_pipeline_final.py --books data/books.csv --customers data/customers.csv
    python library_pipeline_final.py --no-sql
    python library_pipeline_final.py --help
"""

import pandas as pd
import numpy as np
from datetime import datetime
import argparse
import warnings
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
    
    try:
        books_df = pd.read_csv(books_path)
        customers_df = pd.read_csv(customers_path)
        
        print(f"  ✓ Books data loaded: {len(books_df)} rows")
        print(f"  ✓ Customers data loaded: {len(customers_df)} rows")
        
        return books_df, customers_df
        
    except FileNotFoundError as e:
        print(f"  ❌ Error: File not found - {e}")
        raise
    except Exception as e:
        print(f"  ❌ Error loading files: {e}")
        raise


def duplicateCheck(df):
    """
    Check for and remove duplicate records
    AM Task Requirement: duplicateCheck function
    
    Args:
        df: DataFrame to check for duplicates
        
    Returns:
        DataFrame: DataFrame with duplicates removed
    """
    print("\n🔍 Checking for duplicates...")
    
    initial_count = len(df)
    
    # Check for duplicates based on Books, Customer ID, and Checkout date
    duplicates = df.duplicated(subset=['Books', 'Customer ID', 'Book checkout'], keep='first')
    duplicate_count = duplicates.sum()
    
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
    
    Args:
        df: DataFrame to check for NaN values
        
    Returns:
        DataFrame: DataFrame with NaN records removed
    """
    print("\n🔍 Checking for missing values (NaN)...")
    
    initial_count = len(df)
    
    # Count NaN values in critical columns
    na_books = df['Books'].isna().sum()
    na_customers = df['Customer ID'].isna().sum()
    
    print(f"  • Missing book titles: {na_books}")
    print(f"  • Missing customer IDs: {na_customers}")
    
    # Remove rows where critical fields are NaN
    df_clean = df.dropna(subset=['Books', 'Customer ID'], how='any')
    
    removed_count = initial_count - len(df_clean)
    
    print(f"  ✓ Records with NaN removed: {removed_count}")
    print(f"  ✓ Records remaining: {len(df_clean)}")
    
    return df_clean


def dateCleaner(df):
    """
    Clean and format date fields, fix common date issues
    AM Task Requirement: dateCleaner function
    
    Args:
        df: DataFrame with date fields to clean
        
    Returns:
        DataFrame: DataFrame with cleaned dates
    """
    print("\n🧹 Cleaning date fields...")
    
    # Remove extra quotes from dates
    df['Book checkout'] = df['Book checkout'].astype(str).str.replace('"', '', regex=False)
    df['Book Returned'] = df['Book Returned'].astype(str).str.replace('"', '', regex=False)
    
    print("  ✓ Removed extra quotes from dates")
    
    # Fix obvious typos (2063 -> 2023)
    df['Book checkout'] = df['Book checkout'].str.replace('2063', '2023')
    df['Book checkout'] = df['Book checkout'].str.replace('2062', '2023')
    
    print("  ✓ Fixed future date typos (2063 -> 2023)")
    
    # Remove impossible dates (like 32/05/2023)
    initial_count = len(df)
    df = df[~df['Book checkout'].str.contains('32/', na=False)]
    df = df[~df['Book checkout'].str.contains('31/02/', na=False)]
    df = df[~df['Book checkout'].str.contains('31/04/', na=False)]
    
    removed = initial_count - len(df)
    if removed > 0:
        print(f"  ✓ Removed {removed} records with impossible dates")
    
    # Strip whitespace from book titles
    df['Books'] = df['Books'].astype(str).str.strip()
    
    # Convert to datetime
    try:
        df['Book checkout'] = pd.to_datetime(df['Book checkout'], format='%d/%m/%Y', errors='coerce')
        df['Book Returned'] = pd.to_datetime(df['Book Returned'], format='%d/%m/%Y', errors='coerce')
        print("  ✓ Dates converted to datetime format")
    except Exception as e:
        print(f"  ⚠️  Warning during date conversion: {e}")
    
    # Remove rows where date conversion failed
    initial_count = len(df)
    df = df.dropna(subset=['Book checkout'])
    removed = initial_count - len(df)
    if removed > 0:
        print(f"  ✓ Removed {removed} records with invalid dates")
    
    return df


def dataEnrich(df):
    """
    Calculate days between checkout and return dates, add enrichment fields
    AM Task Requirement: dataEnrich() - Calculate days between dates
    
    Args:
        df: DataFrame with date columns
        
    Returns:
        DataFrame: Enriched DataFrame with calculated fields
    """
    print("\n📊 Enriching data with calculated fields...")
    
    # REQUIRED: Calculate loan duration (days between dates)
    df['loan_duration'] = (df['Book Returned'] - df['Book checkout']).dt.days
    print("  ✓ Calculated loan_duration (days between checkout and return)")
    
    # Remove negative loan durations (return before checkout)
    initial_count = len(df)
    df = df[df['loan_duration'] >= 0]
    removed = initial_count - len(df)
    if removed > 0:
        print(f"  ✓ Removed {removed} records with negative loan duration")
    
    # Additional enrichments for better analysis
    df['checkout_date_iso'] = df['Book checkout'].dt.strftime('%Y-%m-%d')
    df['return_date_iso'] = df['Book Returned'].dt.strftime('%Y-%m-%d')
    
    # Calculate expected return date (14 days from checkout)
    df['expected_return_date'] = df['Book checkout'] + pd.Timedelta(days=14)
    
    # Calculate overdue days
    df['overdue_days'] = (df['Book Returned'] - df['expected_return_date']).dt.days
    
    # Flag overdue books
    df['is_overdue'] = df['overdue_days'] > 0
    
    # Statistics
    avg_duration = df['loan_duration'].mean()
    overdue_count = df['is_overdue'].sum()
    overdue_pct = (overdue_count / len(df) * 100) if len(df) > 0 else 0
    
    print(f"  ✓ Added 6 calculated fields:")
    print(f"    • loan_duration (required)")
    print(f"    • checkout_date_iso (SQL compatible)")
    print(f"    • return_date_iso (SQL compatible)")
    print(f"    • expected_return_date")
    print(f"    • overdue_days")
    print(f"    • is_overdue")
    
    print(f"\n  📈 Statistics:")
    print(f"    • Average loan duration: {avg_duration:.1f} days")
    print(f"    • Overdue books: {overdue_count} ({overdue_pct:.1f}%)")
    
    return df


# ============================================================================
# SECTION 2: DATA ENGINEERING METRICS TRACKING
# ============================================================================

class DEMetrics:
    """Track Data Engineering metrics throughout the pipeline"""
    
    def __init__(self):
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
        print("DATA ENGINEERING METRICS SUMMARY".center(70))
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
        """Convert metrics to dictionary for SQL insert"""
        self.calculate_totals()
        return {
            'timestamp': self.timestamp,
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
# SECTION 3: SQL SERVER INTEGRATION
# ============================================================================

def create_database_if_not_exists(server='localhost', database='DE5_Module5'):
    """
    Create the database if it doesn't exist
    
    Args:
        server: SQL Server instance name
        database: Database name to create
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        import pyodbc
        
        # Connect to master database to create our database
        conn_string = (
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server={server};"
            f"Database=master;"
            f"Trusted_Connection=yes;"
        )
        
        conn = pyodbc.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{database}'")
        if cursor.fetchone():
            print(f"  ✓ Database '{database}' already exists")
        else:
            # Create database
            cursor.execute(f"CREATE DATABASE {database}")
            print(f"  ✓ Database '{database}' created successfully")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"  ⚠️  Could not create database: {e}")
        return False


def write_to_sql_server(books_df, customers_df, metrics, server='localhost', database='DE5_Module5'):
    """
    Write cleaned data to SQL Server (SSMS)
    AM Task Requirement: Write to local SQL Server
    
    Args:
        books_df: Cleaned books DataFrame
        customers_df: Cleaned customers DataFrame
        metrics: DEMetrics object
        server: SQL Server instance name
        database: Database name
    """
    print("\n" + "="*70)
    print("LOADING DATA TO SQL SERVER".center(70))
    print("="*70)
    
    try:
        from sqlalchemy import create_engine
        import pyodbc
        
        # Step 1: Create database if it doesn't exist
        print("\n🔧 Checking database...")
        create_database_if_not_exists(server, database)
        
        # Step 2: Create connection string with Windows Authentication
        connection_string = f'mssql+pyodbc://@{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
        
        # Step 3: Create engine and test connection
        engine = create_engine(connection_string)
        
        # Test the connection
        with engine.connect() as connection:
            print(f"  ✓ Connected to SQL Server: {server}/{database}")
        
        # Step 4: Write books data
        print("\n📚 Writing books data...")
        books_df.to_sql('books_bronze', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Loaded {len(books_df)} records to 'books_bronze' table")
        
        # Step 5: Write customers data
        print("\n👥 Writing customers data...")
        customers_df.to_sql('customers_bronze', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Loaded {len(customers_df)} records to 'customers_bronze' table")
        
        # Step 6: Write DE metrics log
        print("\n📊 Writing DE metrics log...")
        metrics_df = pd.DataFrame([metrics.to_dict()])
        metrics_df.to_sql('DE_metrics_log', con=engine, if_exists='append', index=False)
        print(f"  ✓ Logged metrics to 'DE_metrics_log' table")
        
        print("\n" + "="*70)
        print("✅ SQL SERVER LOAD COMPLETE".center(70))
        print("="*70)
        
        return True
        
    except ImportError as e:
        print(f"\n⚠️  Missing required packages: {e}")
        print("\n   Install with:")
        print("   pip install sqlalchemy pyodbc")
        print("\n   Skipping SQL Server load...")
        return False
        
    except Exception as e:
        error_msg = str(e).lower()
        print(f"\n❌ Error writing to SQL Server: {e}")
        print("\n   📋 TROUBLESHOOTING GUIDE:")
        
        if 'login failed' in error_msg:
            print("   ❌ LOGIN FAILED - Possible causes:")
            print("      1. User 'MIS\\Admin' doesn't have SQL Server access")
            print("      2. Windows Authentication not enabled")
            print("\n   ✅ SOLUTIONS:")
            print("      Option A - Run SQL Server Management Studio (SSMS) as Administrator:")
            print("         1. Right-click SSMS → 'Run as administrator'")
            print("         2. Connect to your SQL Server")
            print("         3. Run the setup_database.sql script provided")
            print("\n      Option B - Use the --no-sql flag to skip SQL Server:")
            print("         python library_pipeline_final.py --no-sql")
            print("\n      Option C - Grant permissions (in SSMS):")
            print("         GRANT ALL ON DATABASE::DE5_Module5 TO [MIS\\Admin];")
            
        elif 'cannot open database' in error_msg or 'database' in error_msg:
            print("   ❌ DATABASE NOT FOUND - The database doesn't exist")
            print("\n   ✅ SOLUTION - Create the database:")
            print("      1. Open SQL Server Management Studio (SSMS)")
            print("      2. Open the file: setup_database.sql")
            print("      3. Execute the script (F5)")
            print("      4. Re-run this pipeline")
            print("\n      Or run this SQL command in SSMS:")
            print("         CREATE DATABASE DE5_Module5;")
            
        elif 'driver' in error_msg or 'odbc' in error_msg:
            print("   ❌ DRIVER NOT FOUND")
            print("\n   ✅ SOLUTION - Install ODBC Driver:")
            print("      Download: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server")
            
        else:
            print("   ❌ GENERAL ERROR")
            print("\n   ✅ QUICK SOLUTIONS:")
            print("      1. Ensure SQL Server is running")
            print("      2. Run setup_database.sql in SSMS first")
            print("      3. Use --no-sql flag to skip SQL and save to CSV only")
            
        print("\n   💡 TIP: Use --no-sql flag to skip SQL Server:")
        print("      python library_pipeline_final.py --no-sql")
        print("      This will save to CSV files instead.")
        
        return False


# ============================================================================
# SECTION 4: MAIN PIPELINE
# ============================================================================

def run_pipeline(books_path, customers_path, save_to_sql=True, server='localhost', database='DE5_Module5'):
    """
    Run the complete data quality pipeline
    
    Args:
        books_path: Path to books CSV file
        customers_path: Path to customers CSV file
        save_to_sql: Whether to save to SQL Server
        server: SQL Server instance name
        database: Database name
        
    Returns:
        tuple: (books_df, customers_df, metrics)
    """
    print("\n" + "="*70)
    print("LIBRARY DATA QUALITY PIPELINE - STARTING".center(70))
    print("="*70)
    
    # Initialize metrics tracker
    metrics = DEMetrics()
    
    # Step 1: Load data (fileLoader)
    books_df, customers_df = fileLoader(books_path, customers_path)
    metrics.initial_rows = len(books_df)
    
    # Step 2: Remove duplicates (duplicateCheck)
    books_df = duplicateCheck(books_df)
    metrics.rows_after_duplicates = len(books_df)
    metrics.duplicates_removed = metrics.initial_rows - metrics.rows_after_duplicates
    
    # Step 3: Remove NaN values (naCheck)
    books_df = naCheck(books_df)
    metrics.rows_after_na = len(books_df)
    metrics.na_removed = metrics.rows_after_duplicates - metrics.rows_after_na
    
    # Step 4: Clean dates (dateCleaner)
    books_df = dateCleaner(books_df)
    metrics.rows_after_cleaning = len(books_df)
    metrics.invalid_dates_removed = metrics.rows_after_na - metrics.rows_after_cleaning
    
    # Step 5: Enrich data (dataEnrich)
    books_df = dataEnrich(books_df)
    metrics.final_rows = len(books_df)
    metrics.negative_duration_removed = metrics.rows_after_cleaning - metrics.final_rows
    
    # Step 6: Clean customers data
    print("\n👥 Cleaning customers data...")
    customers_df = customers_df.dropna()
    print(f"  ✓ Valid customers: {len(customers_df)}")
    
    # Step 7: Print metrics
    metrics.print_summary()
    
    # Step 8: Save to CSV
    print("\n💾 Saving cleaned data to CSV files...")
    books_df.to_csv('books_cleaned_final.csv', index=False)
    customers_df.to_csv('customers_cleaned_final.csv', index=False)
    print("  ✓ Saved: books_cleaned_final.csv")
    print("  ✓ Saved: customers_cleaned_final.csv")
    
    # Step 9: Write to SQL Server (if enabled)
    if save_to_sql:
        write_to_sql_server(books_df, customers_df, metrics, server, database)
    
    print("\n" + "="*70)
    print("✅ PIPELINE COMPLETE".center(70))
    print("="*70)
    
    return books_df, customers_df, metrics


# ============================================================================
# SECTION 5: COMMAND LINE INTERFACE
# ============================================================================

def main():
    """
    Main function with argparse for command-line execution
    STRETCH GOAL: argparse library for CLI with file path specification
    """
    parser = argparse.ArgumentParser(
        description='Library Data Quality Pipeline - Process and clean library data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with default paths:
  python %(prog)s
  
  # Specify custom file paths:
  python %(prog)s --books data/books.csv --customers data/customers.csv
  
  # Specify SQL Server details:
  python %(prog)s --server localhost --database LibraryDB
  
  # Skip SQL Server load:
  python %(prog)s --no-sql
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
    
    # SQL Server arguments
    parser.add_argument(
        '--server',
        default='localhost',
        help='SQL Server instance name (default: localhost)'
    )
    
    parser.add_argument(
        '--database',
        default='DE5_Module5',
        help='Database name (default: DE5_Module5)'
    )
    
    parser.add_argument(
        '--no-sql',
        action='store_true',
        help='Skip SQL Server load, only save to CSV'
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Run pipeline with provided arguments
    books_df, customers_df, metrics = run_pipeline(
        books_path=args.books,
        customers_path=args.customers,
        save_to_sql=not args.no_sql,
        server=args.server,
        database=args.database
    )
    
    return books_df, customers_df, metrics


if __name__ == "__main__":
    # Run with command-line arguments
    books_df, customers_df, metrics = main()