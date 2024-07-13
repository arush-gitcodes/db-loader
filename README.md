Data Loader Project
<br>

This project involves developing a Python code to load data files from a ZIP archive into a PostgreSQL database.
<br>
Key Features
<br>
1. Reading Configuration and Mapping Files
   <br>

    Implemented functions to read a JSON configuration file and a table-column mapping file.
   <br>
    The configuration file includes:
   <br>
        Database connection details
   <br>
        Data file paths
   <br>
        Customizable delimiters used in the data records (e.g., .dat files with column delimiter: ~##~ and row delimiter: ~@^*^@~).
   <br>
    The mapping file includes:
        Names of the tables and their columns to be extracted from the data files and inserted into the PostgreSQL Database.
   <br>
        Required tables are created in PostgreSQL before data insertion.
   <br>

3. Extracting Files
   <br>

    Developed a method to extract files from a ZIP archive to a specified directory.
   <br>

5. Database Connectivity
   <br>

    Established a connection to PostgreSQL using the psycopg2 library with credentials from the configuration file.
   <br>

7. Data Processing and Insertion
   <br>

    Implemented logic to read data from extracted files, considering customized delimiters.
   <br>
    Dynamically generated SQL insert queries based on the table-column mapping file.
   <br>
    Ensured data consistency by handling potential issues like column count mismatches.
   <br>

9. Logging and Error Handling
    <br>

    Added error handling for database operations and other potential exceptions.
   <br>
    Configured logging to track the execution flow and catch any errors or warnings.
   

Prerequisites

    Python 3.x
    PostgreSQL
    Required Python libraries: psycopg2, json, zipfile, logging
