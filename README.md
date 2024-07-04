Data Loader Project

This project involves developing a Python code to load data files from a ZIP archive into a PostgreSQL database.
Key Features
1. Reading Configuration and Mapping Files

    Implemented functions to read a JSON configuration file and a table-column mapping file.
    The configuration file includes:
        Database connection details
        Data file paths
        Customizable delimiters used in the data records (e.g., .dat files with column delimiter: ~##~ and row delimiter: ~@^*^@~).
    The mapping file includes:
        Names of the tables and their columns to be extracted from the data files and inserted into the PostgreSQL Database.
        Required tables are created in PostgreSQL before data insertion.

2. Extracting Files

    Developed a method to extract files from a ZIP archive to a specified directory.

3. Database Connectivity

    Established a connection to PostgreSQL using the psycopg2 library with credentials from the configuration file.

4. Data Processing and Insertion

    Implemented logic to read data from extracted files, considering customized delimiters.
    Dynamically generated SQL insert queries based on the table-column mapping file.
    Ensured data consistency by handling potential issues like column count mismatches.

5. Logging and Error Handling

    Added error handling for database operations and other potential exceptions.
    Configured logging to track the execution flow and catch any errors or warnings.

Prerequisites

    Python 3.x
    PostgreSQL
    Required Python libraries: psycopg2, json, zipfile, logging
