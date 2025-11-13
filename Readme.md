Author: Geoff Daly

-   Purpose:
    End-to-end data quality pipeline for a library system that cleans, enriches and loads books/checkout and customer CSV data.

-   File loading (fileLoader)
    Reads the books and customers CSVs.
    Logs basic info, drops fully empty rows, and returns two DataFrames.

-   Books data cleaning:
    Duplicate removal (duplicateCheck)
    Removes duplicate rows based on Books, Customer ID, and Book checkout.
    Missing values (naCheck)
    Changes Customer ID to numeric.
    Drops rows where both Books and Customer ID are missing, keeps others.

-   Date cleaning (dateCleaner)
    Strips quotes/whitespace and standardises empty date values.
    Fixes obvious year typos (2062/2063 → 2023).
    Fixes impossible dates (e.g. 32/05 → 31/05).
    Converts Book checkout and Book Returned to datetime, leaving invalids as NaT (no row drops).
    Keeps _raw copies of the original date strings.

-   Data enrichment (dataEnrich)
    Adds calculated fields without dropping rows:
    loan_duration (days between checkout and return, correcting swapped dates).
    negative_duration_flag (still-bad durations).
    ISO-formatted dates for easier SQL use.
    expected_return_date (checkout + 14 days).
    overdue_days and is_overdue (handles unreturned books using “today”).

-   Customers data cleaning:
    For now: only ensures fully empty rows are dropped (already done in fileLoader).

-   Metrics tracking (DEMetrics)
    Tracks row counts after each stage (duplicates, NaNs, cleaning, enrichment) for both books and customers.
    Calculates total dropped records and retention rate.
    Prints a formatted summary and can output metrics as a dict for logging.

-   SQL Server integration:
    create_database_if_not_exists: ensures the target database exists (via pyodbc).
    write_to_sql_server:
    Connects via SQLAlchemy + pyodbc.
    Writes cleaned books to books_bronze, customers to customers_bronze.
    Appends DE metrics to DE_metrics_log.
    Includes detailed error handling and troubleshooting hints (login, DB missing, driver).

-   Output files:
    Always writes cleaned CSVs:
    books_cleaned_final.csv
    customers_cleaned_final.csv

-   CLI / script entrypoint (main)
    Uses argparse to accept:
    --books, --customers for CSV paths.
    --server, --database for SQL Server details.
    --no-sql to skip SQL load (only save CSVs).

-   Containerised version (Docker) Created
    Packaged
    Always writes cleaned CSVs:
    books_cleaned_final.csv
    customers_cleaned_final.csv    

Running the script (python Library_Data_Pipeline.py [...]) executes the full pipeline with these options.
