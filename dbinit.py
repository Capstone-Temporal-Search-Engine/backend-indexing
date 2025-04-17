import os
import psycopg2
from dotenv import load_dotenv

def create_tables():
    # Load environment variables from .env file
    load_dotenv()

    # Get database credentials from environment variables with defaults
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER", "") or ""
    DB_PASSWORD = os.getenv("DB_PASSWORD", "") or ""
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")

    # Validate required fields
    if not all([DB_NAME, DB_HOST, DB_PORT]):
        raise ValueError("Missing one or more required database credentials (DB_NAME, DB_HOST, DB_PORT)")

    # Define the SQL commands
    sql_commands = """
    DO $$ BEGIN
        DROP TYPE IF EXISTS content_type_enum CASCADE;
        DROP TYPE IF EXISTS priority_enum CASCADE;
        DROP TYPE IF EXISTS approval_status_enum CASCADE;
        DROP TYPE IF EXISTS document_type_enum CASCADE;
    END $$;


    -- Drop and recreate tables
    DROP TABLE IF EXISTS documents CASCADE;
    DROP TABLE IF EXISTS requests CASCADE;


    -- Enum for content type
    CREATE TYPE content_type_enum AS ENUM ('website', 'media', 'document', 'social media post', 'other');

    -- Enum for priority levels
    CREATE TYPE priority_enum AS ENUM ('low', 'medium', 'high');

    -- Enum for approval status
    CREATE TYPE approval_status_enum AS ENUM ('pending', 'approved', 'rejected');

    -- Enum for document type
    CREATE TYPE document_type_enum AS ENUM ('pdf', 'jpg', 'png');

    CREATE TABLE requests (
        request_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        content_type content_type_enum NOT NULL,
        priority priority_enum NOT NULL DEFAULT 'medium',
        content_url TEXT NOT NULL,
        description TEXT,
        admin_notes TEXT,
        email VARCHAR(255) NOT NULL,
        approval_status approval_status_enum NOT NULL DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE documents (
        document_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        request_id UUID NOT NULL,
        document_title VARCHAR(500) NOT NULL,
        document_url VARCHAR(500) NOT NULL,
        document_type document_type_enum NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT fk_request FOREIGN KEY (request_id) REFERENCES requests (request_id) ON DELETE CASCADE
    );
    """
    try:
        # Establish database connection
        connection = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cursor = connection.cursor()

        # Execute the SQL commands
        cursor.execute(sql_commands)
        connection.commit()
        print("Tables and enums created successfully.")
    
    except Exception as e:
        print(f"❌ Error occurred: {e}")
    
    finally:
        # Close the cursor and connection
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()
            print("✅ Database connection closed.")

if __name__ == "__main__":
    create_tables()