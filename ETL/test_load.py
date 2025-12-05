# test_collect.py
import pandas as pd
from load import get_db_connection, filter_volcanoes_by_names, load_csv_to_postgres
import psycopg2
import pytest

def test_get_db_connection():
    """Test basic PostgreSQL connection"""
    try:
        conn = psycopg2.connect(
            dbname="volcanic_etl",
            user="test",
            password="test_password",
            host="localhost"
        )
        conn.close()
        assert True, "Connection succeeded"
    except Exception as e:
        assert False, f"Connection failed: {e}"


