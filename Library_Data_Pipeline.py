"""
Library Data Quality Analysis Pipeline
Author: Geoff Daly
Email: Geoff.daly@moto-way.co.uk
Project: DataEngProdDev

Description:
    Automated data quality pipeline for library management system.
    Validates, cleans, transforms, and loads library checkout data.

Usage:
    python library_data_pipeline.py
"""

import pandas as pd
import numpy as np
from datetime import datetime
import re
import os


# ============================================================================
# SECTION 1: DATA VALIDATION FUNCTIONS
# ============================================================================

def validate_date_format(date_string):
    """
    Validate if a date string is in DD/MM/YYYY format
    
    Args:
        date_string: String to validate
        
    Returns:
        tuple: (is_valid, error_message)
    """
    if pd.isna(date_string):
        return False, "Date is missing (NaN)"
    
    # Remove extra quotes if present
    date_string = str(date_string).replace('"', '').strip()
    
    # Check format with regex: DD/MM/YYYY
    pattern = r'^\d{2}/\d{2}/\d{4}$'
    if not re.match(pattern, date_string):
        return False, f"Invalid date format: {date_string}. Expected DD/MM/YYYY"
    
    return True, None


def validate_date_range(date_string):
    """
    Validate that date is not in the future and is reasonable
    
    Args:
        date_string: Date string in DD/MM/YYYY format
        
    Returns:
        tuple: (is_valid, error_message)
    """
    if pd.isna(date_string):
        return False, "Date is missing (NaN)"
    
    date_string = str(date_string).replace('"', '').strip()
    
    try:
        date_obj = datetime.strptime(date_string, '%d/%m/%Y')
        current_date = datetime.now()
        
        if date_obj > current_date:
            return False, f"Future date detected: {date_string}"
        
        if date_obj.year < 2000:
            return False, f"Date too old: {date_string}"
        
        return True, None
        
    except ValueError as e:
        return False, f"Invalid date value: {date_string} - {str(e)}"


def validate_impossible_dates(date_string):
    """
    Check for impossible dates like 32/05/2023
    
    Args:
        date_string: Date string to validate
        
    Returns:
        tuple: (is_valid, error_message)
    """
    if pd.isna(date_string):
        return False, "Date is missing (NaN)"
    
    date_string = str(date_string).replace('"', '').strip()
    
    try:
        datetime.strptime(date_string, '%d/%m/%Y')
        return True, None
    except ValueError:
        return False, f"Impossible date: {date_string}"


def validate_return_after_checkout(checkout_date, return_date):
    """
    Validate that return date is after checkout date
    
    Args:
        checkout_date: Checkout date string
        return_date: Return date string
        
    Returns:
        tuple: (is_valid, error_message)
    """
    if pd.isna(checkout_date) or pd.isna(return_date):
        return True, None
    
    checkout_date = str(checkout_date).replace('"', '').strip()
    return_date = str(return_date).replace('"', '').strip()
    
    try:
        checkout_obj = datetime.strptime(checkout_date, '%d/%m/%Y')
        return_obj = datetime.strptime(return_date, '%d/%m/%Y')
        
        if return_obj < checkout_obj:
            return False, f"Return date ({return_date}) is before checkout date ({checkout_date})"
        
        return True, None
        
    except ValueError as e:
        return False, f"Error parsing dates: {str(e)}"


def validate_customer_reference(customer_id, customers_df):
    """
    Validate that customer ID exists in the customers table
    
    Args:
        customer_id: Customer ID to validate
        customers_df: DataFrame containing customer data
        
    Returns:
        tuple: (is_valid, error_message)
    """
    if pd.isna(customer_id):
        return False, "Customer ID is missing (NaN)"
    
    try:
        customer_id = int(customer_id)
    except (ValueError, TypeError):
        return False, f"Invalid customer ID format: {customer_id}"
    
    if customer_id in customers_df['Customer ID'].values:
        return True, None
    else:
        return False, f"Customer ID {customer_id} not found in customers table"


def detect_duplicates(books_df):
    """
    Detect duplicate checkout records
    
    Args:
        books_df: Books checkout DataFrame
        
    Returns:
        list: List of duplicate row indices
    """
    duplicates = []
    
    for idx, row in books_df.iterrows():
        if pd.isna(row['Books']) or pd.isna(row['Customer ID']):
            continue
            
        matching_rows = books_df[
            (books_df['Books'] == row['Books']) &
            (books_df['Customer ID'] == row['Customer ID']) &
            (books_df['Book checkout'] == row['Book checkout'])
        ]
        
        if len(matching_rows) > 1 and idx != matching_rows.index[0]:
            duplicates.append({
                'row': idx + 2,
                'book': row['Books'],
                'customer_id': int(row['Customer ID']) if not pd.isna(row['Customer ID']) else 'N/A',
                'checkout_date': row['Book checkout'],
                'duplicate_of_row': matching_rows.index[0] + 2
            })
    
    return duplicates


def detect_formatting_issues(books_df):
    """
    Detect formatting issues like extra quotes and trailing spaces
    
    Args:
        books_df: Books checkout DataFrame
        
    Returns:
        list: List of formatting issues found
    """
    issues = []
    
    for idx, row in books_df.iterrows():
        # Check for extra quotes in dates
        if not pd.isna(row['Book checkout']) and '"""' in str(row['Book checkout']):
            issues.append({
                'row': idx + 2,
                'field': 'Book checkout',
                'issue': 'Extra quotes in date field'
            })
        
        if not pd.isna(row['Book Returned']) and '"""' in str(row['Book Returned']):
            issues.append({
                'row': idx + 2,
                'field': 'Book Returned',
                'issue': 'Extra quotes in date field'
            })
        
        # Check for trailing/leading spaces in book titles
        if not pd.isna(row['Books']):
            book_title = str(row['Books'])
            if book_title != book_title.strip():
                issues.append({
                    'row': idx + 2,
                    'field': 'Books',
                    'issue': f'Trailing/leading spaces in title: "{book_title}"'
                })
    
    return issues


def validate_dataframe(books_df, customers_df):
    """
    Validate entire books dataframe and return detailed error report
    
    Args:
        books_df: Books checkout DataFrame
        customers_df: Customers DataFrame
        
    Returns:
        dict: Dictionary containing validation results and errors
    """
    errors = {
        'date_format_errors': [],
        'date_range_errors': [],
        'impossible_dates': [],
        'logical_date_errors': [],
        'customer_reference_errors': [],
        'duplicates': [],
        'formatting_issues': [],
        'total_errors': 0
    }
    
    print("Starting data validation...")
    print(f"Total records to validate: {len(books_df)}")
    
    for idx, row in books_df.iterrows():
        # Validate checkout date format
        is_valid, error_msg = validate_date_format(row['Book checkout'])
        if not is_valid:
            errors['date_format_errors'].append({
                'row': idx + 2,
                'error': error_msg
            })
        
        # Validate checkout date range
        is_valid, error_msg = validate_date_range(row['Book checkout'])
        if not is_valid:
            errors['date_range_errors'].append({
                'row': idx + 2,
                'error': error_msg
            })
        
        # Validate impossible dates
        is_valid, error_msg = validate_impossible_dates(row['Book checkout'])
        if not is_valid:
            errors['impossible_dates'].append({
                'row': idx + 2,
                'error': error_msg
            })
        
        # Validate return date (if exists)
        if not pd.isna(row['Book Returned']):
            is_valid, error_msg = validate_impossible_dates(row['Book Returned'])
            if not is_valid:
                errors['impossible_dates'].append({
                    'row': idx + 2,
                    'field': 'Return Date',
                    'error': error_msg
                })
        
        # Validate logical date order
        is_valid, error_msg = validate_return_after_checkout(
            row['Book checkout'], 
            row['Book Returned']
        )
        if not is_valid:
            errors['logical_date_errors'].append({
                'row': idx + 2,
                'error': error_msg
            })
        
        # Validate customer reference
        is_valid, error_msg = validate_customer_reference(
            row['Customer ID'], 
            customers_df
        )
        if not is_valid:
            errors['customer_reference_errors'].append({
                'row': idx + 2,
                'error': error_msg
            })
    
    # Detect duplicates
    print("Checking for duplicates...")
    errors['duplicates'] = detect_duplicates(books_df)
    
    # Detect formatting issues
    print("Checking for formatting issues...")
    errors['formatting_issues'] = detect_formatting_issues(books_df)
    
    # Calculate total errors
    errors['total_errors'] = (
        len(errors['date_format_errors']) +
        len(errors['date_range_errors']) +
        len(errors['impossible_dates']) +
        len(errors['logical_date_errors']) +
        len(errors['customer_reference_errors']) +
        len(errors['duplicates']) +
        len(errors['formatting_issues'])
    )
    
    print(f"\nValidation complete!")
    print(f"Total errors found: {errors['total_errors']}")
    
    return errors


# ============================================================================
# SECTION 2: DATA CLEANING FUNCTIONS
# ============================================================================

def remove_empty_rows(df):
    """
    Remove rows where all data columns are NaN
    
    Args:
        df: DataFrame to clean
        
    Returns:
        DataFrame: Cleaned dataframe
    """
    # Remove rows where Books and Customer ID are both NaN
    initial_count = len(df)
    df_cleaned = df.dropna(subset=['Books', 'Customer ID'], how='all')
    removed_count = initial_count - len(df_cleaned)
    
    print(f"  Removed {removed_count} empty rows")
    return df_cleaned


def remove_duplicates(df):
    """
    Remove duplicate records based on book, customer, and checkout date
    
    Args:
        df: DataFrame to clean
        
    Returns:
        DataFrame: Cleaned dataframe
    """
    initial_count = len(df)
    df_cleaned = df.drop_duplicates(subset=['Books', 'Customer ID', 'Book checkout'], keep='first')
    removed_count = initial_count - len(df_cleaned)
    
    print(f"  Removed {removed_count} duplicate records")
    return df_cleaned


def clean_date_formatting(df):
    """
    Remove extra quotes and clean date formatting
    
    Args:
        df: DataFrame to clean
        
    Returns:
        DataFrame: Cleaned dataframe
    """
    # Remove extra quotes from date columns
    df['Book checkout'] = df['Book checkout'].astype(str).str.replace('"""', '').str.replace('"', '')
    df['Book Returned'] = df['Book Returned'].astype(str).str.replace('"""', '').str.replace('"', '')
    
    print(f"  Cleaned date formatting (removed extra quotes)")
    return df


def clean_text_fields(df):
    """
    Remove trailing/leading whitespace from text fields
    
    Args:
        df: DataFrame to clean
        
    Returns:
        DataFrame: Cleaned dataframe
    """
    df['Books'] = df['Books'].astype(str).str.strip()
    
    print(f"  Cleaned text fields (removed trailing spaces)")
    return df


def fix_future_dates(df):
    """
    Fix obvious date errors (e.g., 2063 -> 2023)
    
    Args:
        df: DataFrame to clean
        
    Returns:
        DataFrame: Cleaned dataframe
    """
    # Fix 2063 -> 2023 (obvious typo)
    df['Book checkout'] = df['Book checkout'].astype(str).str.replace('2063', '2023')
    df['Book Returned'] = df['Book Returned'].astype(str).str.replace('2063', '2023')
    
    print(f"  Fixed future dates (2063 -> 2023)")
    return df


def handle_impossible_dates(df):
    """
    Remove or flag records with impossible dates
    
    Args:
        df: DataFrame to clean
        
    Returns:
        DataFrame: Cleaned dataframe
    """
    initial_count = len(df)
    
    # Remove rows with impossible dates (can't be fixed)
    df = df[~df['Book checkout'].astype(str).str.contains('32/', na=False)]
    
    removed_count = initial_count - len(df)
    print(f"  Removed {removed_count} records with impossible dates")
    return df


def clean_dataframe(books_df):
    """
    Apply all cleaning functions to the dataframe
    
    Args:
        books_df: Raw books DataFrame
        
    Returns:
        DataFrame: Cleaned dataframe
    """
    print("\nStarting data cleaning...")
    print(f"Initial record count: {len(books_df)}")
    
    # Apply cleaning steps in order
    books_df = remove_empty_rows(books_df)
    books_df = remove_duplicates(books_df)
    books_df = clean_date_formatting(books_df)
    books_df = clean_text_fields(books_df)
    books_df = fix_future_dates(books_df)
    books_df = handle_impossible_dates(books_df)
    
    print(f"Final record count: {len(books_df)}")
    print(f"Total records cleaned: {len(books_df)}")
    
    return books_df


# ============================================================================
# SECTION 3: DATA TRANSFORMATION FUNCTIONS
# ============================================================================

def standardize_dates(df):
    """
    Convert dates from DD/MM/YYYY to YYYY-MM-DD (ISO format for SQL)
    
    Args:
        df: DataFrame with dates to standardize
        
    Returns:
        DataFrame: DataFrame with standardized dates
    """
    print("\nStandardizing dates to ISO format (YYYY-MM-DD)...")
    
    def convert_date(date_str):
        if pd.isna(date_str) or date_str == 'nan' or date_str == '':
            return None
        try:
            date_obj = datetime.strptime(str(date_str), '%d/%m/%Y')
            return date_obj.strftime('%Y-%m-%d')
        except:
            return None
    
    df['CheckoutDate_ISO'] = df['Book checkout'].apply(convert_date)
    df['ReturnDate_ISO'] = df['Book Returned'].apply(convert_date)
    
    print(f"  Converted {df['CheckoutDate_ISO'].notna().sum()} checkout dates")
    print(f"  Converted {df['ReturnDate_ISO'].notna().sum()} return dates")
    
    return df


def calculate_loan_metrics(df):
    """
    Calculate loan duration and overdue status
    
    Args:
        df: DataFrame with date columns
        
    Returns:
        DataFrame: DataFrame with calculated metrics
    """
    print("\nCalculating loan metrics...")
    
    # Calculate expected return date (checkout + 14 days)
    df['ExpectedReturnDate'] = pd.to_datetime(df['CheckoutDate_ISO']) + pd.Timedelta(days=14)
    
    # Calculate actual loan duration
    df['ActualLoanDays'] = (
        pd.to_datetime(df['ReturnDate_ISO']) - pd.to_datetime(df['CheckoutDate_ISO'])
    ).dt.days
    
    # Calculate overdue days
    df['OverdueDays'] = (
        pd.to_datetime(df['ReturnDate_ISO']) - df['ExpectedReturnDate']
    ).dt.days
    
    # Flag overdue books (overdue days > 0)
    df['IsOverdue'] = df['OverdueDays'] > 0
    
    overdue_count = df['IsOverdue'].sum()
    print(f"  Identified {overdue_count} overdue book returns")
    
    return df


def generate_data_quality_report(errors, initial_count, final_count):
    """
    Generate a summary report of data quality issues and cleaning results
    
    Args:
        errors: Dictionary of validation errors
        initial_count: Initial record count
        final_count: Final record count after cleaning
        
    Returns:
        dict: Summary report
    """
    report = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'initial_records': initial_count,
        'final_records': final_count,
        'records_removed': initial_count - final_count,
        'errors_found': errors['total_errors'],
        'error_breakdown': {
            'date_format_errors': len(errors['date_format_errors']),
            'date_range_errors': len(errors['date_range_errors']),
            'impossible_dates': len(errors['impossible_dates']),
            'logical_date_errors': len(errors['logical_date_errors']),
            'customer_reference_errors': len(errors['customer_reference_errors']),
            'duplicates': len(errors['duplicates']),
            'formatting_issues': len(errors['formatting_issues'])
        },
        'data_quality_score': round((final_count / initial_count) * 100, 2) if initial_count > 0 else 0
    }
    
    return report


# ============================================================================
# SECTION 4: SQL SERVER LOADING FUNCTIONS
# ============================================================================

def get_sql_connection(server='localhost', database='LibraryDataQuality', create_db_if_not_exists=True):
    """
    Create connection to SQL Server database
    Creates the database automatically if it doesn't exist
    
    Args:
        server: SQL Server instance name (default: localhost)
        database: Database name (default: LibraryDataQuality)
        create_db_if_not_exists: If True, creates database if it doesn't exist
        
    Returns:
        pyodbc connection object or None if failed
    """
    try:
        import pyodbc
        
        # Try multiple ODBC drivers (18, 17, or generic)
        drivers = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "SQL Server"
        ]
        
        working_driver = None
        for driver in drivers:
            try:
                # Test connection to master database
                test_conn_string = (
                    f"Driver={{{driver}}};"
                    f"Server={server};"
                    f"Database=master;"
                    f"Trusted_Connection=yes;"
                    f"TrustServerCertificate=yes;"
                )
                test_conn = pyodbc.connect(test_conn_string, timeout=5)
                test_conn.close()
                working_driver = driver
                print(f"Using ODBC driver: {driver}")
                break
            except:
                continue
        
        if not working_driver:
            raise Exception("No compatible ODBC driver found. Install ODBC Driver 17 or 18 for SQL Server.")
        
        # Try to connect to target database
        conn_string = (
            f"Driver={{{working_driver}}};"
            f"Server={server};"
            f"Database={database};"
            f"Trusted_Connection=yes;"
            f"TrustServerCertificate=yes;"
        )
        
        try:
            conn = pyodbc.connect(conn_string)
            print(f"✓ Connected to SQL Server: {server}/{database}")
            return conn
            
        except pyodbc.Error as db_error:
            # Check if database doesn't exist
            error_msg = str(db_error).lower()
            if create_db_if_not_exists and ("cannot open database" in error_msg or 
                                            "database" in error_msg and "does not exist" in error_msg):
                print(f"⚠️  Database '{database}' not found. Creating it...")
                
                # Connect to master database
                master_conn_string = (
                    f"Driver={{{working_driver}}};"
                    f"Server={server};"
                    f"Database=master;"
                    f"Trusted_Connection=yes;"
                    f"TrustServerCertificate=yes;"
                )
                
                master_conn = pyodbc.connect(master_conn_string)
                master_conn.autocommit = True
                cursor = master_conn.cursor()
                
                try:
                    # Check if database exists
                    cursor.execute(f"SELECT database_id FROM sys.databases WHERE name = '{database}'")
                    if not cursor.fetchone():
                        # Create database
                        cursor.execute(f"CREATE DATABASE [{database}]")
                        print(f"✓ Created database: {database}")
                    else:
                        print(f"✓ Database {database} already exists")
                except Exception as create_error:
                    print(f"✗ Error creating database: {str(create_error)}")
                    cursor.close()
                    master_conn.close()
                    return None
                
                cursor.close()
                master_conn.close()
                
                # Now connect to the new database
                conn = pyodbc.connect(conn_string)
                print(f"✓ Connected to SQL Server: {server}/{database}")
                return conn
            else:
                raise
        
    except ImportError:
        print("✗ ERROR: pyodbc not installed")
        print("   Install with: pip install pyodbc")
        return None
    except pyodbc.Error as e:
        print(f"✗ ERROR: Could not connect to SQL Server")
        print(f"   Error: {str(e)}")
        print("\nTroubleshooting:")
        print("  1. Make sure SQL Server is running")
        print("  2. Try different server names:")
        print("     - localhost")
        print("     - localhost\\SQLEXPRESS")
        print("     - (localdb)\\MSSQLLocalDB")
        print("     - .\\SQLEXPRESS")
        print("  3. Verify Windows Authentication is enabled")
        print("  4. Install ODBC Driver 17 or 18 for SQL Server")
        print("  5. Check SQL Server Configuration Manager")
        return None
    except Exception as e:
        print(f"✗ ERROR: Unexpected error: {str(e)}")
        return None


def create_tables_if_not_exist(conn):
    """
    Create necessary tables in SQL Server if they don't exist
    
    Args:
        conn: pyodbc connection object
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        cursor = conn.cursor()
        
        print("\nCreating tables if they don't exist...")
        
        # Create Books table
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Books')
            BEGIN
                CREATE TABLE Books (
                    BookID INT PRIMARY KEY IDENTITY(1,1),
                    BookTitle NVARCHAR(255) NOT NULL,
                    CreatedDate DATETIME DEFAULT GETDATE()
                )
            END
        """)
        print("  ✓ Books table ready")
        
        # Create Customers table
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Customers')
            BEGIN
                CREATE TABLE Customers (
                    CustomerID INT PRIMARY KEY,
                    CustomerName NVARCHAR(255) NOT NULL,
                    CreatedDate DATETIME DEFAULT GETDATE()
                )
            END
        """)
        print("  ✓ Customers table ready")
        
        # Create BookCheckouts table
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'BookCheckouts')
            BEGIN
                CREATE TABLE BookCheckouts (
                    CheckoutID INT PRIMARY KEY IDENTITY(1,1),
                    BookTitle NVARCHAR(255) NOT NULL,
                    CustomerID INT NOT NULL,
                    CheckoutDate DATE NOT NULL,
                    ReturnDate DATE NULL,
                    ExpectedReturnDate DATE NULL,
                    ActualLoanDays INT NULL,
                    OverdueDays INT NULL,
                    IsOverdue BIT DEFAULT 0,
                    CreatedDate DATETIME DEFAULT GETDATE(),
                    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
                )
            END
        """)
        print("  ✓ BookCheckouts table ready")
        
        # Create DataQualityLog table
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DataQualityLog')
            BEGIN
                CREATE TABLE DataQualityLog (
                    LogID INT PRIMARY KEY IDENTITY(1,1),
                    BatchID UNIQUEIDENTIFIER DEFAULT NEWID(),
                    SourceFile NVARCHAR(255),
                    RecordsProcessed INT,
                    RecordsCleaned INT,
                    RecordsInserted INT,
                    ErrorsFound INT,
                    DataQualityScore DECIMAL(5,2),
                    ProcessingDate DATETIME DEFAULT GETDATE(),
                    Status NVARCHAR(50)
                )
            END
        """)
        print("  ✓ DataQualityLog table ready")
        
        # Create ErrorLog table for detailed error tracking
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ErrorLog')
            BEGIN
                CREATE TABLE ErrorLog (
                    ErrorID INT PRIMARY KEY IDENTITY(1,1),
                    BatchID UNIQUEIDENTIFIER,
                    ErrorType NVARCHAR(100),
                    ErrorMessage NVARCHAR(MAX),
                    RowNumber INT,
                    LogDate DATETIME DEFAULT GETDATE()
                )
            END
        """)
        print("  ✓ ErrorLog table ready")
        
        conn.commit()
        print("✓ All tables created successfully\n")
        return True
        
    except Exception as e:
        print(f"✗ Error creating tables: {str(e)}")
        return False


def load_data_to_sql_server(books_df, customers_df, report, server='localhost', database='LibraryDataQuality'):
    """
    Main function to load all data to SQL Server
    Automatically creates database and tables if they don't exist
    
    Args:
        books_df: Cleaned books DataFrame
        customers_df: Customers DataFrame
        report: Data quality report dictionary
        server: SQL Server instance name
        database: Database name
        
    Returns:
        bool: True if successful, False otherwise
    """
    print_section_header("LOADING DATA TO SQL SERVER")
    
    # Connect to SQL Server (will create database if needed)
    conn = get_sql_connection(server, database, create_db_if_not_exists=True)
    if conn is None:
        print("\n⚠️  Skipping SQL Server loading (connection failed)")
        print("   Data has been saved to CSV file instead")
        return False
    
    try:
        # Create tables if they don't exist
        if not create_tables_if_not_exist(conn):
            print("✗ Failed to create tables")
            conn.close()
            return False
        
        # Load data
        print("Loading data to tables...")
        books_inserted = load_books_to_sql(books_df, conn)
        customers_inserted = load_customers_to_sql(customers_df, conn)
        checkouts_inserted = load_checkouts_to_sql(books_df, conn)
        
        total_inserted = books_inserted + customers_inserted + checkouts_inserted
        
        # Log to DataQualityLog table with BatchID
        import uuid
        batch_id = str(uuid.uuid4())
        log_to_data_quality_table(
            conn,
            batch_id,
            '03_Library_Systembook.csv',
            report['initial_records'],
            report['final_records'],
            checkouts_inserted,
            report['errors_found'],
            report['data_quality_score'],
            'Completed'
        )
        
        # Close connection
        conn.close()
        
        print(f"\n✅ Successfully loaded data to SQL Server!")
        print(f"   Total records inserted: {total_inserted}")
        print(f"     - Unique books: {books_inserted}")
        print(f"     - Customers: {customers_inserted}")
        print(f"     - Checkouts: {checkouts_inserted}")
        print(f"   Batch ID: {batch_id}")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Error loading data to SQL Server: {str(e)}")
        if conn:
            conn.close()
        return False


def log_to_data_quality_table(conn, batch_id, source_file, records_processed, records_cleaned, 
                               records_inserted, errors_found, data_quality_score, status):
    """
    Log processing results to DataQualityLog table
    
    Args:
        conn: pyodbc connection object
        batch_id: Unique identifier for this batch
        source_file: Name of source CSV file
        records_processed: Total records processed
        records_cleaned: Records after cleaning
        records_inserted: Records successfully inserted
        errors_found: Total errors found
        data_quality_score: Data quality percentage
        status: Processing status (Completed/Failed)
    """
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO DataQualityLog 
            (BatchID, SourceFile, RecordsProcessed, RecordsCleaned, RecordsInserted, 
             ErrorsFound, DataQualityScore, Status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (batch_id, source_file, records_processed, records_cleaned, records_inserted, 
              errors_found, data_quality_score, status))
        
        conn.commit()
        print(f"\n✓ Logged processing results to DataQualityLog table")
        
    except Exception as e:
        print(f"⚠️  Warning: Could not log to DataQualityLog: {str(e)}")


def load_books_to_sql(df, conn):
    """
    Load book data to SQL Server
    
    Args:
        df: DataFrame containing book data
        conn: pyodbc connection object
        
    Returns:
        int: Number of records inserted
    """
    cursor = conn.cursor()
    
    print("\nLoading books to SQL Server...")
    
    # Get unique books
    unique_books = df['Books'].unique()
    inserted = 0
    
    for book_title in unique_books:
        if pd.notna(book_title) and book_title != 'nan':
            # Check if book already exists
            cursor.execute(
                "SELECT COUNT(*) FROM Books WHERE BookTitle = ?",
                (book_title,)
            )
            exists = cursor.fetchone()[0]
            
            if not exists:
                cursor.execute(
                    "INSERT INTO Books (BookTitle) VALUES (?)",
                    (book_title,)
                )
                inserted += 1
    
    conn.commit()
    print(f"  âœ“ Inserted {inserted} unique books")
    return inserted


def load_customers_to_sql(customers_df, conn):
    """
    Load customer data to SQL Server
    
    Args:
        customers_df: DataFrame containing customer data
        conn: pyodbc connection object
        
    Returns:
        int: Number of records inserted
    """
    cursor = conn.cursor()
    
    print("\nLoading customers to SQL Server...")
    inserted = 0
    
    for idx, row in customers_df.iterrows():
        if pd.notna(row['Customer ID']) and pd.notna(row['Customer Name']):
            customer_id = int(row['Customer ID'])
            customer_name = row['Customer Name']
            
            # Check if customer already exists
            cursor.execute(
                "SELECT COUNT(*) FROM Customers WHERE CustomerID = ?",
                (customer_id,)
            )
            exists = cursor.fetchone()[0]
            
            if not exists:
                cursor.execute(
                    "INSERT INTO Customers (CustomerID, CustomerName) VALUES (?, ?)",
                    (customer_id, customer_name)
                )
                inserted += 1
    
    conn.commit()
    print(f"  âœ“ Inserted {inserted} customers")
    return inserted


def load_checkouts_to_sql(df, conn):
    """
    Load checkout data to SQL Server
    
    Args:
        df: DataFrame containing checkout data
        conn: pyodbc connection object
        
    Returns:
        int: Number of records inserted
    """
    cursor = conn.cursor()
    
    print("\nLoading checkouts to SQL Server...")
    inserted = 0
    
    for idx, row in df.iterrows():
        if pd.notna(row['Books']) and pd.notna(row['Customer ID']):
            try:
                cursor.execute("""
                    INSERT INTO BookCheckouts 
                    (BookTitle, CustomerID, CheckoutDate, ReturnDate, ExpectedReturnDate, 
                     ActualLoanDays, OverdueDays, IsOverdue)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row['Books'],
                    int(row['Customer ID']),
                    row['CheckoutDate_ISO'],
                    row['ReturnDate_ISO'] if pd.notna(row['ReturnDate_ISO']) else None,
                    row['ExpectedReturnDate'].strftime('%Y-%m-%d') if pd.notna(row['ExpectedReturnDate']) else None,
                    int(row['ActualLoanDays']) if pd.notna(row['ActualLoanDays']) else None,
                    int(row['OverdueDays']) if pd.notna(row['OverdueDays']) else None,
                    bool(row['IsOverdue']) if pd.notna(row['IsOverdue']) else False
                ))
                inserted += 1
            except Exception as e:
                print(f"  Warning: Could not insert row {idx}: {str(e)}")
                continue
    
    conn.commit()
    print(f"  âœ“ Inserted {inserted} checkout records")
    return inserted


def log_to_data_quality_table(conn, source_file, records_processed, records_cleaned, 
                               records_inserted, errors_found, status):
    """
    Log processing results to DataQualityLog table
    
    Args:
        conn: pyodbc connection object
        source_file: Name of source CSV file
        records_processed: Total records processed
        records_cleaned: Records after cleaning
        records_inserted: Records successfully inserted
        errors_found: Total errors found
        status: Processing status (Completed/Failed)
    """
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO DataQualityLog 
        (SourceFile, RecordsProcessed, RecordsCleaned, RecordsInserted, ErrorsFound, Status)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (source_file, records_processed, records_cleaned, records_inserted, errors_found, status))
    
    conn.commit()
    print(f"\nâœ“ Logged processing results to DataQualityLog table")


def load_data_to_sql_server(books_df, customers_df, report, server='localhost', database='LibraryDataQuality'):
    """
    Main function to load all data to SQL Server
    
    Args:
        books_df: Cleaned books DataFrame
        customers_df: Customers DataFrame
        report: Data quality report dictionary
        server: SQL Server instance name
        database: Database name
        
    Returns:
        bool: True if successful, False otherwise
    """
    print_section_header("LOADING DATA TO SQL SERVER")
    
    # Connect to SQL Server
    conn = get_sql_connection(server, database)
    if conn is None:
        print("\nâš ï¸  Skipping SQL Server loading (connection failed)")
        print("   Data has been saved to CSV file instead")
        return False
    
    try:
        # Create tables
        create_tables_if_not_exist(conn)
        
        # Load data
        books_inserted = load_books_to_sql(books_df, conn)
        customers_inserted = load_customers_to_sql(customers_df, conn)
        checkouts_inserted = load_checkouts_to_sql(books_df, conn)
        
        total_inserted = books_inserted + customers_inserted + checkouts_inserted
        
        # Log to DataQualityLog table
        log_to_data_quality_table(
            conn,
            '03_Library_Systembook.csv',
            report['initial_records'],
            report['final_records'],
            checkouts_inserted,
            report['errors_found'],
            'Completed'
        )
        
        # Close connection
        conn.close()
        
        print(f"\nâœ… Successfully loaded data to SQL Server!")
        print(f"   Total records inserted: {total_inserted}")
        print(f"     - Books: {books_inserted}")
        print(f"     - Customers: {customers_inserted}")
        print(f"     - Checkouts: {checkouts_inserted}")
        
        return True
        
    except Exception as e:
        print(f"\nâŒ Error loading data to SQL Server: {str(e)}")
        if conn:
            conn.close()
        return False


# ============================================================================
# SECTION 5: MAIN EXECUTION
# ============================================================================

def print_section_header(title):
    """Print a formatted section header"""
    print("\n" + "="*70)
    print(title.center(70))
    print("="*70)


def print_validation_summary(errors):
    """Print a summary of validation errors"""
    print("\n" + "-"*70)
    print("VALIDATION SUMMARY")
    print("-"*70)
    print(f"  â€¢ Date format errors: {len(errors['date_format_errors'])}")
    print(f"  â€¢ Date range errors: {len(errors['date_range_errors'])}")
    print(f"  â€¢ Impossible dates: {len(errors['impossible_dates'])}")
    print(f"  â€¢ Logical date errors: {len(errors['logical_date_errors'])}")
    print(f"  â€¢ Missing customer refs: {len(errors['customer_reference_errors'])}")
    print(f"  â€¢ Duplicate records: {len(errors['duplicates'])}")
    print(f"  â€¢ Formatting issues: {len(errors['formatting_issues'])}")
    print(f"\n  TOTAL ERRORS: {errors['total_errors']}")
    print("-"*70)


def print_final_report(report):
    """Print the final data quality report"""
    print_section_header("FINAL DATA QUALITY REPORT")
    print(f"\nProcessing Timestamp: {report['timestamp']}")
    print(f"\nRecords Summary:")
    print(f"  Initial records: {report['initial_records']}")
    print(f"  Final records: {report['final_records']}")
    print(f"  Records removed: {report['records_removed']}")
    print(f"\nErrors Found: {report['errors_found']}")
    for error_type, count in report['error_breakdown'].items():
        print(f"  â€¢ {error_type.replace('_', ' ').title()}: {count}")
    print(f"\nData Quality Score: {report['data_quality_score']}%")
    print("="*70)


def main():
    """
    Main execution function - runs the complete data pipeline
    """
    print_section_header("LIBRARY DATA QUALITY ANALYSIS PIPELINE")
    print(f"Author: Geoff Daly")
    print(f"Project: DataEngProdDev")
    print(f"Current Directory: {os.getcwd()}")
    
    # ========================================================================
    # STEP 1: LOAD DATA
    # ========================================================================
    print_section_header("STEP 1: LOADING DATA")
    
    try:
        # Try both Data/ and data/ folder names
        if os.path.exists('Data'):
            data_folder = 'Data'
        elif os.path.exists('data'):
            data_folder = 'data'
        else:
            raise FileNotFoundError("Neither 'Data' nor 'data' folder found")
        
        books_df = pd.read_csv(f'{data_folder}/03_Library_Systembook.csv')
        customers_df = pd.read_csv(f'{data_folder}/03_Library_SystemCustomers.csv')
        
        print(f"âœ“ Books CSV loaded: {len(books_df)} records")
        print(f"âœ“ Customers CSV loaded: {len(customers_df)} records")
        
        initial_count = len(books_df)
        
    except FileNotFoundError as e:
        print(f"\nâŒ ERROR: {str(e)}")
        print("\nMake sure your CSV files are in a 'Data' or 'data' folder:")
        print("  - 03_Library_Systembook.csv")
        print("  - 03_Library_SystemCustomers.csv")
        return
    
    # ========================================================================
    # STEP 2: VALIDATE DATA
    # ========================================================================
    print_section_header("STEP 2: VALIDATING DATA")
    
    errors = validate_dataframe(books_df, customers_df)
    print_validation_summary(errors)
    
    # ========================================================================
    # STEP 3: CLEAN DATA
    # ========================================================================
    print_section_header("STEP 3: CLEANING DATA")
    
    books_cleaned = clean_dataframe(books_df.copy())
    
    # ========================================================================
    # STEP 4: TRANSFORM DATA
    # ========================================================================
    print_section_header("STEP 4: TRANSFORMING DATA")
    
    books_transformed = standardize_dates(books_cleaned)
    books_transformed = calculate_loan_metrics(books_transformed)
    
    # ========================================================================
    # STEP 5: LOAD TO SQL SERVER
    # ========================================================================
    final_count = len(books_cleaned)
    report = generate_data_quality_report(errors, initial_count, final_count)
    
    print_section_header("STEP 5: LOADING DATA TO SQL SERVER")
    print("\nSQL Server is the primary data destination for this pipeline.")
    print("Connecting to: localhost/LibraryDataQuality")
    print("="*70)
    
    # SQL Server connection is MANDATORY
    server = 'localhost'
    database = 'LibraryDataQuality'
    
    sql_success = load_data_to_sql_server(books_transformed, customers_df, report, server, database)
    
    if not sql_success:
        print("\n" + "="*70)
        print("ERROR: PIPELINE FAILED - Could not connect to SQL Server")
        print("="*70)
        print("\nSQL Server connection is required for this pipeline.")
        print("\nTroubleshooting:")
        print("  1. Verify SQL Server is running")
        print("  2. Check server name - try:")
        print("     - localhost")
        print("     - localhost\\\\SQLEXPRESS")
        print("     - (localdb)\\\\MSSQLLocalDB")
        print("  3. Install pyodbc: pip install pyodbc")
        print("  4. Install ODBC Driver 17 or 18 for SQL Server")
        print("="*70)
        return  # Exit pipeline on SQL Server failure
    
    # ========================================================================
    # STEP 6: GENERATE REPORT
    # ========================================================================
    print_section_header("STEP 6: GENERATING REPORT")
    print_final_report(report)
    
    # ========================================================================
    # COMPLETION
    # ========================================================================
    print_section_header("PIPELINE COMPLETE")
    print("\n[SUCCESS] Data quality pipeline executed successfully!")
    print(f"\nData loaded to SQL Server: {server}/{database}")
    print(f"\nNext Steps:")
    print(f"  1. Query SQL Server tables to review data")
    print(f"  2. Check DataQualityLog table for processing metrics")
    print(f"  3. Create Power BI dashboard connected to SQL Server")
    print(f"  4. Write unit tests for validation functions")
    print(f"  5. Set up CI/CD pipeline automation")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()