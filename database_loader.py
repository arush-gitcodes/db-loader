import psycopg2
import psycopg2.extras
import json
import os
import zipfile
import logging

class DatabaseLoader:
    def __init__(self, config_path, mapping_path):
        self.config = self.read_json(config_path)
        self.table_column_mapping = self.read_json(mapping_path)
        self.setup_logging()

        self.db_host = self.config['host']
        self.db_username = self.config['username']
        self.db_password = self.config['password']
        self.db_port = self.config['port']
        self.db_name = self.config['name']

        self.zip_file_path = self.config['zip_file_path']
        self.row_delimiter = self.config['row_delimiter']
        self.column_delimiter = self.config['column_delimiter']

    @staticmethod
    def read_json(file_path):
        with open(file_path, 'r') as f:
            return json.load(f)

    @staticmethod
    def setup_logging():
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def extract_files(self, extract_to='extracted_data'):
        logging.info(f"Extracting files from {self.zip_file_path} to {extract_to}")
        with zipfile.ZipFile(self.zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)

    def connect_to_db(self):
        logging.info("Connecting to the database")
        return psycopg2.connect(
            dbname=self.db_name,
            user=self.db_username,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port
        )


    def insert_data(self):
        self.extract_files()
        try:
            with self.connect_to_db() as conn:
                with conn.cursor() as cursor:
                    # iterate over tables and columns present in mapping file
                    for table_name, column_names in self.table_column_mapping.items():
                        dat_file_path = os.path.join('../extracted_data', f'{table_name}.dat')

                        if not os.path.isfile(dat_file_path):
                            logging.warning(f"File {dat_file_path} does not exist.")
                            continue

                        # Read data in batches
                        with open(dat_file_path, 'r') as dat_file:
                            data_lines = dat_file.read().split(self.row_delimiter)
                            batch_size = 1000
                            batch = []

                            for line in data_lines:
                                values = line.strip().split(self.column_delimiter)
                                if len(values) != len(column_names):
                                    logging.error(
                                        f"Column count mismatch in {table_name}: {line} "
                                        f"(expected {len(column_names)} columns, got {len(values)} columns)"
                                    )
                                    continue
                                batch.append(tuple(values))

                                if len(batch) >= batch_size:
                                    insert_query = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES %s"
                                    psycopg2.extras.execute_values(cursor, insert_query, batch)
                                    batch = []

                            if batch:
                                insert_query = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES %s"
                                psycopg2.extras.execute_values(cursor, insert_query, batch)

                conn.commit()
                logging.info("Data inserted successfully into all tables.")
        except psycopg2.Error as db_error:
            logging.error(f"Database error: {db_error}")
        except Exception as e:
            logging.error(f"Error: {e}")


