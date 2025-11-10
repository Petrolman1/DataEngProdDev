"""
Data Validation Module
Author: Geoff Daly
Description: Validates library data for quality issues including dates, customer references, and logical consistency
"""

import pandas as pd
from datetime import datetime
import re


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
    
    # Clean the date string
    date_string = str(date_string).replace('"', '').strip()
    
    try:
        # Parse the date
        date_obj = datetime.strptime(date_string, '%d/%m/%Y')
        current_date = datetime.now()
        
        # Check if date is in the future
        if date_obj > current_date:
            return False, f"Future date detected: {date_string}"
        
        # Check if date is before year 2000 (unreasonable for this library system)
        if date_obj.year < 2000:
            return False, f"Date too old: {date_string}"
        
        return True, None
        
    except ValueError as e:
        return False, f"Invalid date value: {date_string} - {str(e)}"


def validate_impossible_dates(date_string):
    """
    Check for impossible dates like 32/05/2023 (32nd of May doesn't exist)
    
    Args:
        date_string: Date string to validate
        
    Returns:
        tuple: (is_valid, error_message)
    """
    if pd.isna(date_string):
        return False, "Date is missing (NaN)"
    
    # Clean the date string
    date_string = str(date_string).replace('"', '').strip()
    
    try:
        # Try to parse - Python will catch impossible dates
        datetime.strptime(date_string, '%d/%m/%Y')
        return True, None
    except ValueError:
        return False, f"Impossible date: {date_string}"


def validate_return_after_checkout(checkout_date, return_date):
    """
    Validate that return date is after checkout date (logical validation)
    
    Args:
        checkout_date: Checkout date string
        return_date: Return date string
        
    Returns:
        tuple: (is_valid, error_message)
    """
    if pd.isna(checkout_date) or pd.isna(return_date):
        return True, None  # Can't validate if one is missing
    
    # Clean the date strings
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
    
    # Convert to int for comparison
    try:
        customer_id = int(customer_id)
    except (ValueError, TypeError):
        return False, f"Invalid customer ID format: {customer_id}"
    
    # Check if customer exists
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
    
    # Check for duplicates based on: Book Title, Customer ID, Checkout Date
    # Keep first occurrence, mark rest as duplicates
    for idx, row in books_df.iterrows():
        if pd.isna(row['Books']) or pd.isna(row['Customer ID']):
            continue  # Skip empty rows
            
        # Find all rows with same book, customer, and checkout date
        matching_rows = books_df[
            (books_df['Books'] == row['Books']) &
            (books_df['Customer ID'] == row['Customer ID']) &
            (books_df['Book checkout'] == row['Book checkout'])
        ]
        
        # If more than one match and this is not the first occurrence
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
                'row': idx + 2,  # +2 because Excel rows start at 1 and header is row 1
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


def print_validation_report(errors):
    """
    Print a nicely formatted validation report
    
    Args:
        errors: Dictionary of validation errors from validate_dataframe()
    """
    print("\n" + "="*70)
    print("DATA QUALITY VALIDATION REPORT")
    print("="*70)
    
    print(f"\nTotal Errors Found: {errors['total_errors']}")
    
    if errors['date_format_errors']:
        print(f"\n❌ Date Format Errors: {len(errors['date_format_errors'])}")
        for error in errors['date_format_errors'][:5]:  # Show first 5
            print(f"   Row {error['row']}: {error['error']}")
    
    if errors['date_range_errors']:
        print(f"\n❌ Date Range Errors: {len(errors['date_range_errors'])}")
        for error in errors['date_range_errors']:
            print(f"   Row {error['row']}: {error['error']}")
    
    if errors['impossible_dates']:
        print(f"\n❌ Impossible Dates: {len(errors['impossible_dates'])}")
        for error in errors['impossible_dates']:
            print(f"   Row {error['row']}: {error['error']}")
    
    if errors['logical_date_errors']:
        print(f"\n❌ Logical Date Errors: {len(errors['logical_date_errors'])}")
        for error in errors['logical_date_errors']:
            print(f"   Row {error['row']}: {error['error']}")
    
    if errors['customer_reference_errors']:
        print(f"\n❌ Customer Reference Errors: {len(errors['customer_reference_errors'])}")
        for error in errors['customer_reference_errors'][:5]:  # Show first 5
            print(f"   Row {error['row']}: {error['error']}")
    
    if errors['duplicates']:
        print(f"\n❌ Duplicate Records: {len(errors['duplicates'])}")
        for dup in errors['duplicates']:
            print(f"   Row {dup['row']}: '{dup['book']}' for Customer {dup['customer_id']} " +
                  f"(duplicate of row {dup['duplicate_of_row']})")
    
    if errors['formatting_issues']:
        print(f"\n❌ Formatting Issues: {len(errors['formatting_issues'])}")
        for issue in errors['formatting_issues'][:10]:  # Show first 10
            print(f"   Row {issue['row']} ({issue['field']}): {issue['issue']}")
    
    print("\n" + "="*70)


# Test function to run validation on the library CSV files
if __name__ == "__main__":
    print("Testing Data Validator Module\n")
    
    # Test individual functions first
    print("Testing individual validation functions:")
    print("-" * 50)
    
    # Test 1: Valid date
    result, msg = validate_date_format("20/02/2023")
    print(f"Test valid date: {'✓ PASS' if result else '✗ FAIL'}")
    
    # Test 2: Future date (from your data - row 8)
    result, msg = validate_date_range("10/04/2063")
    print(f"Test future date: {'✓ PASS' if not result else '✗ FAIL'} - {msg}")
    
    # Test 3: Impossible date (from your data - row 18)
    result, msg = validate_impossible_dates("32/05/2023")
    print(f"Test impossible date: {'✓ PASS' if not result else '✗ FAIL'} - {msg}")
    
    # Test 4: Return before checkout
    result, msg = validate_return_after_checkout("24/03/2023", "21/03/2023")
    print(f"Test logical error: {'✓ PASS' if not result else '✗ FAIL'} - {msg}")
    
    print("\n" + "="*70)
    print("Ready to test with actual CSV files!")
    print("="*70)
    print("\nTo test with your CSV files, run:")
    print("  python test_validator_with_csv.py")