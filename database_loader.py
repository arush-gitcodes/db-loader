import psycopg2
import psycopg2.extras
import json
import os
import zipfile
import logging
from datetime import datetime
import duckdb


class DatabaseLoader:
    def __init__(self, config_path, mapping_path):
        self.config = self.read_json(config_path)
        self.table_column_mapping = self.read_json(mapping_path)

        log_dir = self.config.get('log_dir', None)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        self.setup_logging(log_dir)

        self.log_startup_message()

        self.validate_database()

        self.db_host = self.config['db_host']
        self.db_username = self.config['db_username']
        self.db_password = self.config['db_password']
        self.db_port = self.config['db_port']
        self.db_name = self.config['db_name']
        self.db_schema = self.config.get('db_schema', 'public')  # Use 'public' if no schema is specified

        self.zip_dir = self.config['zip_dir']
        if not os.path.exists(self.zip_dir):
            raise FileNotFoundError(f"ZIP directory not found: {self.zip_dir}")
        self.encoding = self.config.get('encoding', 'utf-8')
        self.row_delimiter = self.config['row_delimiter']
        self.column_delimiter = self.config['column_delimiter']
        self.batch_size = self.config.get('batch_size', 1000)

        self.skip_header_rows = self.config.get('skip_header_rows', 0)
        self.skip_footer_rows = self.config.get('skip_footer_rows', 0)

        self.error_dir = self.config.get('error_dir', None)
        if not os.path.exists(self.error_dir):
            os.makedirs(self.error_dir)

    @staticmethod
    def read_json(file_path):
        with open(file_path, 'r') as f:
            return json.load(f)

    @staticmethod
    def setup_logging(log_dir=None):
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        log_level = logging.INFO

        if log_dir:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file_name = f"db_loader_application_{timestamp}.log"
            log_file_path = os.path.join(log_dir, log_file_name)
            logging.basicConfig(level=log_level, format=log_format, filename=log_file_path, filemode='a')
        else:
            logging.basicConfig(level=log_level, format=log_format)

        console = logging.StreamHandler()
        console.setLevel(log_level)
        console.setFormatter(logging.Formatter(log_format))
        logging.getLogger().addHandler(console)


    def log_startup_message(self):
        startup_message = f"Db_loader_application started at {datetime.now()}"
        logging.info(startup_message)


    def extract_files(self, zip_dir, extract_to='extracted_data'):
        logging.info(f"Extracting files from {zip_dir} to {extract_to}")
        with zipfile.ZipFile(zip_dir, 'r') as zip_ref:
            zip_ref.extractall(extract_to)

    def validate_database(self):
        required_credentials = ['db_host', 'db_username', 'db_password', 'db_port', 'db_name']
        missing_credentials = [cred for cred in required_credentials if
                               cred not in self.config or not self.config[cred]]

        if missing_credentials:
            raise ValueError(f"Missing database credentials: {', '.join(missing_credentials)}")
        logging.info("All required database credentials are provided.")

    def connect_to_db(self):
        logging.info("Connecting to the database")
        return psycopg2.connect(
            dbname=self.db_name,
            user=self.db_username,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port
        )

    def table_exists(self, cursor, table_name):
        cursor.execute(
            f"SELECT EXISTS ("
            f"SELECT 1 FROM information_schema.tables "
            f"WHERE table_schema = %s AND table_name = %s);",
            (self.db_schema, table_name)
        )
        return cursor.fetchone()[0]

    def log_error_to_file(self, table_name, error_message):
        os.makedirs(self.error_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_file_name = f"{table_name}_error_{timestamp}.dat"
        error_file_path = os.path.join(self.error_dir, error_file_name)
        with open(error_file_path, 'a') as error_file:
            error_file.write(f"{datetime.now()} - {error_message}\n")

    def load_data_to_duckdb(self, data, source_columns,sql_query):
        # Create an in-memory DuckDB database
        con = duckdb.connect(database=':memory:')

        # Create a staging table in DuckDB with all source columns
        create_table_query = f"CREATE TABLE staging ({', '.join([f'{col} VARCHAR' for col in source_columns])});"
        con.execute(create_table_query)

        # Insert data into DuckDB staging table
        placeholders = ', '.join(['?'] * len(source_columns))
        insert_query = f"INSERT INTO staging ({', '.join(source_columns)}) VALUES ({placeholders});"
        con.executemany(insert_query, data)

        # Execute the transformation query
        transformation_query = f"SELECT {sql_query} FROM staging;"

        # Execute the transformation query
        transformed_data = con.execute(transformation_query).fetchall()
        con.close()
        return transformed_data

    def insert_data(self):
        # Extract files from the ZIP archive
        self.extract_files(self.zip_dir, extract_to='extracted_data')

        inserted_tables = 0
        try:
            with self.connect_to_db() as conn:
                with conn.cursor() as cursor:

                    for table_name, columns in self.table_column_mapping.items():
                        target_columns = columns['target_columns']
                        source_columns = columns['source_columns']
                        data_file_name = columns['data_file']
                        sql_query = columns['sql_query']

                        # Check if table name present in mapping file exists in PG database
                        if not self.table_exists(cursor, table_name):
                            logging.warning(f"Table {self.db_schema}.{table_name} does not exist.")
                            continue

                        # Determine file path and encoding
                        dat_file_path = os.path.join('extracted_data', f'{data_file_name}.dat')
                        if not os.path.isfile(dat_file_path):
                            logging.warning(
                                f"File for table {table_name} ({data_file_name}.dat) does not exist in the ZIP archive.")
                            continue

                        start_time = datetime.now()
                        logging.info(f"Starting data ingestion for table {self.db_schema}.{table_name} at {start_time}")

                        with open(dat_file_path, 'r', encoding=self.encoding) as dat_file:
                            data_lines = dat_file.read().split(self.row_delimiter)
                            # Skip header and footer rows
                            data_lines = data_lines[self.skip_header_rows:len(data_lines) - self.skip_footer_rows]
                            batch = []

                            for line in data_lines:
                                values = line.strip().split(self.column_delimiter)

                                # Check if the number of values is sufficient before selecting columns
                                if len(values) < len(target_columns):
                                    error_message = (
                                        f"Column count mismatch in {table_name}: {line} "
                                        f"(expected at least {len(target_columns)} columns, got {len(values)} columns)"
                                    )
                                    logging.error(error_message)
                                    self.log_error_to_file(table_name, error_message)
                                    continue

                                selected_values = [values[source_columns.index(col)] for col in source_columns]

                                batch.append(tuple(selected_values))

                                if len(batch) >= self.batch_size:
                                    # Load data to DuckDB, transform and fetch the results
                                    transformed_data = self.load_data_to_duckdb(batch, source_columns,
                                                                                sql_query)

                                    # Insert transformed data to PostgreSQL
                                    insert_query = (
                                        f"INSERT INTO {self.db_schema}.{table_name} ({', '.join(target_columns)}) "
                                        f"VALUES %s"
                                    )

                                    psycopg2.extras.execute_values(cursor, insert_query, transformed_data)
                                    batch = []

                            if batch:
                                # Load remaining data to DuckDB, transform and fetch the results
                                transformed_data = self.load_data_to_duckdb(batch, source_columns, sql_query)

                                # Insert transformed data to PostgreSQL
                                insert_query = (
                                    f"INSERT INTO {self.db_schema}.{table_name} ({', '.join(target_columns)}) "
                                    f"VALUES %s"
                                )

                                psycopg2.extras.execute_values(cursor, insert_query, transformed_data)

                        end_time = datetime.now()
                        logging.info(f"Completed data ingestion for table {self.db_schema}.{table_name} at {end_time}")
                        logging.info(f"Duration: {end_time - start_time}")

                        inserted_tables += 1

                conn.commit()
                logging.info(f"Data inserted successfully into {inserted_tables} tables.")
        except psycopg2.OperationalError as e:
            logging.error(f"Operational error: {e}")
        except psycopg2.IntegrityError as e:
            logging.error(f"Integrity error: {e}")
        except Exception as e:
            logging.error(f"General error: {e}")


