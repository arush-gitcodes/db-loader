# Data Loader Project

This project involves developing a Python code to load data files from a ZIP archive into a PostgreSQL database.

## Key Features

### 1. Reading Configuration and Mapping Files

- **Configuration File**: The configuration file (`config.json`) contains essential settings for the data loader application, including:
  - **Database Connection Details**: Host, username, password, port, and database name.
  - **File Paths**: Path to the ZIP file containing the data files.
  - **Data Delimiters**: Customizable delimiters for rows (`row_delimiter`) and columns (`column_delimiter`) in the data files.
  - **Batch Size**: Number of records to process in a single batch during data insertion.
  - **Logging and Error Handling**: Paths for log and error directories, enabling detailed tracking of the application's execution and error management.
  - **Optional Settings**: Encoding format for reading files, the number of header and footer rows to skip, and the database schema to use (default is `public`).

- **Mapping File**: The mapping file (`mapping.json`) defines the relationship between the source data files and the target PostgreSQL tables, including:
  - **Table and Column Mappings**: Specifies the source columns to extract from the data files and the corresponding target columns in the PostgreSQL tables.
  - **Data Files**: Names of the data files (without extensions) to be processed for each table.
  - **Transformation Queries**: SQL queries to transform the data within an in-memory DuckDB instance before inserting it into PostgreSQL.

### 2. Extracting Files

- Developed a method to extract files from a ZIP archive to a specified directory.

### 3. Database Connectivity

- Established a connection to PostgreSQL using the `psycopg2` library with credentials from the configuration file.

### 4. Data Processing and Insertion

- **Data Extraction**: Extracts data files from the specified ZIP archive to a designated directory (`extracted_data`).

- **Data Reading and Processing**:
  - **File Reading**: Reads the data files using the specified encoding and delimiters. Skips header and footer rows as configured.
  - **Batch Processing**: Processes data in batches to handle large datasets efficiently. Each batch of data is read and split based on the configured row and column delimiters.
  - **Column Mapping**: Selects and maps source columns to target columns according to the mapping file. Checks for column count mismatches to ensure data integrity.

- **Data Transformation with DuckDB**:
  - **Staging Table**: Creates an in-memory staging table in DuckDB with columns matching the source data.
  - **Data Insertion into DuckDB**: Inserts the batch of data into the DuckDB staging table.
  - **Transformation Query**: Applies the transformation query specified in the mapping file to the data in DuckDB.
  - **Fetching Transformed Data**: Retrieves the transformed data from DuckDB for insertion into PostgreSQL.

- **Data Insertion into PostgreSQL**:
  - **Table Existence Check**: Verifies if the target table exists in PostgreSQL before attempting data insertion.
  - **SQL Insert Queries**: Generates and executes SQL insert queries dynamically based on the transformed data and target column mappings.
  - **Error Handling**: Logs errors and warnings during data insertion, including detailed messages for issues like column count mismatches and database errors.

- **Commit and Logging**:
  - **Transaction Management**: Commits the transaction after successfully inserting data into PostgreSQL.
  - **Logging**: Logs detailed information about the data ingestion process, including start and end times, duration, and any errors encountered.

### 5. Logging and Error Handling

- Added error handling for database operations and other potential exceptions.
- Configured logging to track the execution flow and catch any errors or warnings.
- Logged errors to specific files in a designated error directory.

## Prerequisites

- Python 3.x
- PostgreSQL
- Required Python libraries: `psycopg2`, `json`, `zipfile`, `logging`, `duckdb`
