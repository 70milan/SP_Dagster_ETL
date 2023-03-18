import os
from sqlalchemy import create_engine
from configparser import ConfigParser


def postgres_connection():
    config = ConfigParser()
    config.read(os.path.join(os.path.dirname(os.path.abspath(__name__)), 'sp_etl/database.ini'))
    try:
        # Get Postgres connection details from config
        postgres_user = config.get('postgres', 'user')
        postgres_password = config.get('postgres', 'password')
        postgres_host = config.get('postgres', 'host')
        postgres_port = config.get('postgres', 'port')
        postgres_database = config.get('postgres', 'database')
        engine = create_engine(f'postgresql://{postgres_user}:\{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')
        print("Connected to Postgres database successfully!")
        return engine
    except Exception as e:
        print("Failed to connect to Postgres database: ", e)
        return None