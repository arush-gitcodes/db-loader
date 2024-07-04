from finish.database_loader import DatabaseLoader

def main():
    config_path = 'config.json'
    mapping_path = 'table_column_mapping.json'
    loader = DatabaseLoader(config_path, mapping_path)
    loader.insert_data()

if __name__ == "__main__":
    main()
