import psycopg2
from psycopg2 import OperationalError

# Replace with your actual values or use os.environ to load from .env
DB_HOST = "127.0.0.1"
DB_NAME = "tp_search_db"
DB_USER = ""  # or your macOS username, e.g. 'alex'
DB_PASSWORD = ""
DB_PORT = 5432

def test_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        print("✅ Connection successful!")
        conn.close()
    except OperationalError as e:
        print("❌ Connection failed!")
        print(e)

if __name__ == "__main__":
    test_connection()