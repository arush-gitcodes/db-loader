from finish.database_loader import DatabaseLoader
import logging

def main():
    config_path = 'config.json'
    mapping_path = 'table_column_mapping.json'
    try:
        loader = DatabaseLoader(config_path, mapping_path)
    except ValueError as e:
        logging.error(f"Configuration error: {e}")
        return
    except FileNotFoundError as e:
        logging.error(f"File error: {e}")
        return
    loader.insert_data()

if __name__ == "__main__":
    main()
